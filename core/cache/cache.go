package cache

import (
	"sync"
	"mydb/core/indexes"
	"mydb/core/pages"
	"mydb/core/prims"
	"mydb/core/types"
)

const (
	CACHE_SIZE = 256
	CACHE_MIN  = 25
	MAX_DELETE = 200
	SWEEP_INTERVAL = 1000
)

type cacheEntry struct {
	p 		types.PageLike
	prev 	*cacheEntry
	next 	*cacheEntry
	used	uint8
}

type Cache struct {
	fd 			int
	m 			map[uint64]*cacheEntry
	total 		uint16
	mux 		*sync.RWMutex
	oldest 		*cacheEntry
	newest 		*cacheEntry
	sinceSweep	uint64

	buffPool 	*Pool[[]byte]
	entryPool 	*Pool[*cacheEntry]
	pagePool 	*Pool[*pages.Page]

	bBasePool	*Pool[*indexes.BTItemBase]
	nodePool 	*Pool[*indexes.BTreeNode]
	leafPool 	*Pool[*indexes.BTreeLeaf]

}

func NewCache(fd int) *Cache { 
	return &Cache{ 
		fd: fd,
		m:make(map[uint64]*cacheEntry, CACHE_SIZE),
		total: 0,
		mux: &sync.RWMutex{},
		oldest: nil,
		newest: nil,

		buffPool: 	NewBufferPool(CACHE_SIZE),
		entryPool: 	NewCacheEntryPool(CACHE_SIZE),
		bBasePool: NewBaseItemPool(CACHE_SIZE),

		nodePool: 	NewNodePool(CACHE_SIZE),
		leafPool: 	NewLeafPool(CACHE_SIZE),
		pagePool: 	NewPagePool(CACHE_SIZE),
	}
}

func (c *Cache) GetBuffer() ([]byte) { return c.buffPool.Get() }
func (c *Cache) PutBuffer(buff []byte) { c.buffPool.Put(buff) }

func (c *Cache) PutNode(node *indexes.BTreeNode) { c.nodePool.Put(node) }
func (c *Cache) GetNode(id uint64) *indexes.BTreeNode { 
	n, ok := c.Get(id)
	if !ok {
		node := c.nodePool.Get() 
		buff := c.buffPool.Get()
		offset:= int64(id * uint64(pages.PAGE_SIZE))

		_, err := prims.Read(c.fd, buff, offset)
		if err != nil {
			c.nodePool.Put(node)
			c.buffPool.Put(buff)
			return nil
		}

		err = node.FromBytes(buff)
		if err != nil {
			c.nodePool.Put(node)
			c.buffPool.Put(buff)
			return nil
		}

		return node
	} else {
		return n.(*indexes.BTreeNode)
	}
}

func (c *Cache) PutLeaf(leaf *indexes.BTreeLeaf) { c.leafPool.Put(leaf) }
func (c *Cache) GetLeaf(id uint64) *indexes.BTreeLeaf { 
	l, ok := c.Get(id)
	if !ok {
		lea := c.leafPool.Get() 
		buff := c.buffPool.Get()
		offset:= int64(id * uint64(pages.PAGE_SIZE))

		_, err := prims.Read(c.fd, buff, offset)
		if err != nil {
			c.leafPool.Put(lea)
			c.buffPool.Put(buff)
			return nil
		}

		err = lea.FromBytes(buff)
		if err != nil {
			c.leafPool.Put(lea)
			c.buffPool.Put(buff)
			return nil
		}

		return lea
	} else {
		return l.(*indexes.BTreeLeaf)
	}
}

func (c *Cache) PutPage(p *pages.Page) { c.pagePool.Put(p) }
func (c *Cache) GetPage(id uint64) *pages.Page { 
	p, ok := c.Get(id)
	if !ok {
		pa := c.pagePool.Get() 
		buff := c.buffPool.Get()
		offset:= int64(id * uint64(pages.PAGE_SIZE))

		_, err := prims.Read(c.fd, buff, offset)
		if err != nil {
			c.pagePool.Put(pa)
			c.buffPool.Put(buff)
			return nil
		}

		err = pa.FromBytes(buff)
		if err != nil {
			c.pagePool.Put(pa)
			c.buffPool.Put(buff)
			return nil
		}

		return pa
	} else {
		return p.(*pages.Page)
	}
}

func (c *Cache) Get(id uint64) (types.PageLike, bool) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	e, ok := c.m[id]
	if !ok { return nil, false }

	if e.used < 5 {
		e.used ++
	}
	c.sinceSweep++

	// Move the entry to the front of the cache
	if c.newest != e {
		e.prev.next = e.next
		if e.next != nil { e.next.prev = e.prev }
		if c.oldest == e { c.oldest = e.prev }
		e.prev = nil
		e.next = c.newest
		if c.newest != nil { c.newest.prev = e }
		c.newest = e
	}
	return e.p, true
}

func (c *Cache) Set(item types.PageLike) {
	c.mux.Lock()
	defer c.mux.Unlock()
	e := c.entryPool.Get()
	e.p = item
	e.prev = nil
	e.next = c.newest

	e.used = 5

	c.m[item.GetId()] = e

	if c.newest != nil { c.newest.prev = e }
	c.newest = e
	c.total++

	if c.total >= CACHE_SIZE {
		c.HandleFullCache()
	} else if c.total == 1 {
		c.oldest = e 
	} else if c.sinceSweep >= SWEEP_INTERVAL {
		c.RunSweep()
	}
}

func (c *Cache) RunSweep() {
	c.sinceSweep = 0
	node := c.newest
	for node != nil {
		node.used--
		if node.used == 0 {
			if !node.p.InUse() {
				buff := node.p.ToBytes()
				if node.p.IsDirty() {
					node.p.Lock()
					node.p.Flush(buff)
					node.p.Unlock()
				}
				c.PutBuffer(buff)
				c.ReturnToPool(node.p)

				delete(c.m, node.p.GetId())

				if node.prev != nil {
					node.prev.next = node.next
				} else {
					c.newest = node.next
				}
				if node.next != nil {
					node.next.prev = node.prev
				} else {
					c.oldest = node.prev
				}

				c.entryPool.Put(node)
				c.total--
			} else {
				node.used++ 
			}
		}
		node = node.next
	}
}

func (c *Cache)HandleFullCache() {
	node := c.oldest
	var deletedCount int
	for c.total > CACHE_MIN && node != nil {
		if !node.p.InUse() { 
			var buff []byte
			buff = node.p.ToBytes()
			if node.p.IsDirty() { 
				node.p.Lock()
				node.p.Flush(buff)
				node.p.Unlock()
			}

			if deletedCount <= MAX_DELETE {
				c.PutBuffer(buff)
				delete(c.m, node.p.GetId())
				if node.prev != nil {
					node.prev.next = node.next
				} else {
					c.newest = node.next
				}
				if node.next != nil {
					node.next.prev = node.prev
				} else {
					c.oldest = node.prev
				}
				c.entryPool.Put(node)
				deletedCount++
				c.total--
			}
		}
		node = node.prev
	}
}

func (c *Cache) ReturnToPool(p types.PageLike) {
	switch p.GetType() {
	case pages.IDX_LEAF:
		c.bBasePool.Put(p.(*indexes.BTreeLeaf).BTItemBase)
		c.leafPool.Put(p.(*indexes.BTreeLeaf))
	case pages.IDX_NODE:
		c.bBasePool.Put(p.(*indexes.BTreeNode).BTItemBase)
		c.nodePool.Put(p.(*indexes.BTreeNode))
	default:
		c.pagePool.Put(p.(*pages.Page))
	}
}

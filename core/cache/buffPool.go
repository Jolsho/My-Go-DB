package cache

import (
	"sync"
	"mydb/core/indexes"
	"mydb/core/pages"
)

type Pool[T any] struct {
	stack []T
	*sync.Cond
}

func (bp *Pool[T]) Get() T {
	bp.L.Lock()
	defer bp.L.Unlock()
	for len(bp.stack) < 1 {
		bp.Wait()
	}
	n := len(bp.stack)
	buf := bp.stack[n-1]
	bp.stack = bp.stack[:n-1]
	return buf
}

func (bp *Pool[T]) Put(buf T) {
	bp.L.Lock()
	defer bp.L.Unlock()
	bp.stack = append(bp.stack, buf)
	bp.Signal()
}

func NewBufferPool(count int) *Pool[[]byte] {
	pool := &Pool[[]byte]{
		stack: make([][]byte, 0, count),
		Cond: &sync.Cond{
			L: &sync.Mutex{},
		},
	}
	for range count {
		pool.stack = append(pool.stack, make([]byte, pages.PAGE_SIZE))
	}
	return pool
}

func NewBaseItemPool(count int) *Pool[*indexes.BTItemBase] {
	pool := &Pool[*indexes.BTItemBase]{
		stack: make([]*indexes.BTItemBase, 0, count),
		Cond: &sync.Cond{
			L: &sync.Mutex{},
		},
	}
	for range count {
		pool.stack = append(pool.stack, new(indexes.BTItemBase))
	}
	return pool
}

func NewNodePool(count int) *Pool[*indexes.BTreeNode] {
	pool := &Pool[*indexes.BTreeNode]{
		stack: make([]*indexes.BTreeNode, 0, count),
		Cond: &sync.Cond{
			L: &sync.Mutex{},
		},
	}
	for range count {
		pool.stack = append(pool.stack, new(indexes.BTreeNode))
	}
	return pool
}

func NewLeafPool(count int) *Pool[*indexes.BTreeLeaf] {
	pool := &Pool[*indexes.BTreeLeaf]{
		stack: make([]*indexes.BTreeLeaf, 0, count),
		Cond: &sync.Cond{
			L: &sync.Mutex{},
		},
	}
	for range count {
		pool.stack = append(pool.stack, new(indexes.BTreeLeaf))
	}
	return pool
}

func NewPagePool(count int) *Pool[*pages.Page] {
	pool := &Pool[*pages.Page]{
		stack: make([]*pages.Page, 0, count),
		Cond: &sync.Cond{
			L: &sync.Mutex{},
		},
	}
	for range count {
		pool.stack = append(pool.stack, new(pages.Page))
	}
	return pool
}

func NewCacheEntryPool(count int) *Pool[*cacheEntry] {
	pool := &Pool[*cacheEntry]{
		stack: make([]*cacheEntry, 0, count),
		Cond: &sync.Cond{
			L: &sync.Mutex{},
		},
	}
	for range count {
		pool.stack = append(pool.stack, new(cacheEntry))
	}
	return pool
}


package indexes

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"mydb/core/pages"
	"mydb/core/types"
)

type BTreeItem interface {
	types.PageLike
	init(fd int, entrySizes ...int)
	IsLeaf() bool
	ToBytes() []byte
	Flush([]byte) error
	SetLsn(uint64)

	GetId() uint64
	GetType() pages.PageType
	FromBytes([]byte) error
	IsDirty() bool
	SetIsDirty(bool)
	RLock()
	RUnlock()
	Lock()
	Unlock()
}

const ( 
	// header
	BNODE_HEADER_LENGTH = 1 + 8 + 2 + 2 + 2 + 8
	PAGETYPE_OFFSET = 0
	ID_OFFSET = 1
	KEY_SIZE_OFFSET = 9
	ENTRY_SIZE_OFFSET = 11
	N_OFFSET 	= 13
	LSN_OFFSET = 15

	// sizes
	N_SIZE = 2
	CHILD_SIZE = 8
	
	// node
	KEYS_OFFSET = BNODE_HEADER_LENGTH
)

type BTItemBase struct {
	fd 			int
	isDirty 	bool
	Lsn 		uint64
	PType  		pages.PageType
	Id     		uint64
	N 			uint16
	KeySize 	uint16
	EntrySize 	uint16
	Max 		uint16
	fullBuff 	[]byte
	*LeafLock

}
func (n *BTItemBase) IsDirty() bool { return n.isDirty }
func (n *BTItemBase) SetIsDirty(is bool) { n.isDirty = is }
func (n *BTItemBase) SetLsn(lsn uint64) { n.Lsn = lsn }
func (n *BTItemBase) GetId() (uint64) { return n.Id }
func (n *BTItemBase) GetType() (pages.PageType) { return n.PType }

func (n *BTItemBase) fromBytes(buff []byte) {
	n.PType = pages.PageType(buff[PAGETYPE_OFFSET])
	n.Id = binary.LittleEndian.Uint64(buff[ID_OFFSET:])
	n.KeySize = binary.LittleEndian.Uint16(buff[KEY_SIZE_OFFSET:])
	n.EntrySize = binary.LittleEndian.Uint16(buff[ENTRY_SIZE_OFFSET:])
	n.N = binary.LittleEndian.Uint16(buff[N_OFFSET:])
	n.Lsn = binary.LittleEndian.Uint64(buff[LSN_OFFSET:])
	n.LeafLock = NewLeafLock(n.Id)
	n.fullBuff = buff
}

func (n *BTItemBase) WriteToBuffer(buff []byte) {
	buff[PAGETYPE_OFFSET] = byte(n.PType)
	binary.LittleEndian.PutUint64(buff[ID_OFFSET:],n.Id)
	binary.LittleEndian.PutUint16(buff[KEY_SIZE_OFFSET:],n.KeySize)
	binary.LittleEndian.PutUint16(buff[ENTRY_SIZE_OFFSET:],n.EntrySize)
	binary.LittleEndian.PutUint16(buff[N_OFFSET:], n.N)
	binary.LittleEndian.PutUint64(buff[LSN_OFFSET:], n.Lsn)
}

func (b *BTItemBase)GetParent(q *IdxQuery) (*BTreeNode, error){
	curr := q.GetRoot()
	if curr.GetId() == b.Id { return nil, nil }
	prev, ok := curr.(*BTreeNode)
	if !ok {
		fmt.Println(b.PType.String())
		return nil, nil
	}
	var i int
	var err error
	for {
		if node, ok := curr.(*BTreeNode); ok {
			node.RLock()
			i = 0
			// Find the last key less than or equal to the target
			// if key is less than the first key, we are at the right page
			for i < int(node.N) && bytes.Compare(q.key, node.Keys[i]) > 0 { 
				i++ 
			}

			// Else move to the child node which is closer to the target
			// once new node is a leaf it will return itself
			curr, err = LoadItem(q, node.Children[i])
			node.RUnlock()
			if err != nil { 
				return nil, err 
			}

			if curr.GetId() == b.Id { return prev, nil }
			prev = node

		} else {
			return prev, nil
		}
	}
}


////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////


type LeafLock struct {
	lock     *sync.RWMutex    // primary lock for the node
	stateMu  *sync.Mutex    // guards the below fields
	waiting  bool
	on       uint64
	self     uint64
	cond     *sync.Cond    // uses stateMu
	inUse		bool
	readers 	uint16
}

func NewLeafLock(id uint64) *LeafLock {
	ll := &LeafLock{
		self: id,
		lock: &sync.RWMutex{},
		stateMu: &sync.Mutex{},
		waiting: false,
		on: 0,
		inUse: false,
		readers: 0,
	}
	ll.cond = sync.NewCond(ll.stateMu)
	return ll
}

func (l *LeafLock) Lock()   { 
	l.ToggleInUse()
	l.lock.Lock() 
}
func (l *LeafLock) Unlock() { 
	l.ToggleInUse()
	l.lock.Unlock() 
}
func (l *LeafLock) RLock()   { 
	l.lock.RLock() 
	if !l.inUse {
		l.ToggleInUse()
	}
	l.readers++
}
func (l *LeafLock) RUnlock() { 
	l.lock.RUnlock() 
	if l.readers == 1 {
		l.ToggleInUse()
	}
	l.readers--
}
func (n *LeafLock) InUse() bool { return n.inUse }
func (n *LeafLock) ToggleInUse() { n.inUse = !n.inUse }

func (l *LeafLock) LockSibling(other *LeafLock) {
	l.stateMu.Lock()
	l.waiting = true
	l.on = other.self
	l.stateMu.Unlock()

	// Avoid mutual waiting deadlock
	other.stateMu.Lock()
	l.stateMu.Lock()
	if other.waiting && other.on == l.self {
		l.Unlock() // release the lock to avoid deadlock
		for other.waiting && other.on == l.self {
			l.cond.Wait()
		}
		l.Lock() // re-acquire the lock
	}
	l.stateMu.Unlock()
	other.stateMu.Unlock()

	other.lock.Lock()
}

func (l *LeafLock) UnlockSibling(other *LeafLock) {
	l.stateMu.Lock()
	l.waiting = false
	l.on = 0
	l.stateMu.Unlock()

	other.stateMu.Lock()
	other.cond.Signal()
	other.stateMu.Unlock()

	other.lock.Unlock()
}

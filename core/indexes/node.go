package indexes

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"mydb/core/pages"
	"mydb/core/prims"
	t "mydb/core/types"
)

////////////////////////////////////////////////////
// 				REGULAR NODE					 //
//         { header, n , keys, children }  		//
//  header = {isLeaf, parentId, pageId, keySize } //
////////////////////////////////////////////////////

type BTreeNode struct { 
	*BTItemBase 
	Keys [][]byte // keys in the node
	Children []uint64 // children in the node
}
func (l *BTreeNode) IsLeaf() bool {return false}
func (n *BTreeNode) FromBytes(buff []byte) error {
	if len(buff) < BNODE_HEADER_LENGTH { return t.ErrInvalidPage }
	ItemB := new(BTItemBase)
	ItemB.fromBytes(buff[:])
	n.BTItemBase = ItemB
	n.Max = 0
	n.Keys = make([][]byte, n.N)
	n.Children = make([]uint64, n.N+1)
	cursor := uint16(BNODE_HEADER_LENGTH)
	var i int
	for ;i < int(n.N); {
		n.Keys[i] = buff[cursor:cursor+n.KeySize]
		cursor += n.KeySize
		n.Children[i] = binary.LittleEndian.Uint64(buff[cursor:])
		cursor += CHILD_SIZE
		i++
	}
	// last child
	n.Children[i] = binary.LittleEndian.Uint64(buff[cursor:])
	return nil
}
func (n *BTreeNode) ToBytes() []byte {
	n.WriteToBuffer(n.fullBuff)
	return n.fullBuff
}

func (n *BTreeNode) WriteToBuffer(buff []byte) {
	buff[PAGETYPE_OFFSET] = byte(n.PType)
	binary.LittleEndian.PutUint64(buff[ID_OFFSET:],n.Id)
	binary.LittleEndian.PutUint16(buff[KEY_SIZE_OFFSET:],n.KeySize)
	binary.LittleEndian.PutUint16(buff[ENTRY_SIZE_OFFSET:],n.EntrySize)
	binary.LittleEndian.PutUint16(buff[N_OFFSET:], n.N)
	binary.LittleEndian.PutUint64(buff[LSN_OFFSET:], n.Lsn)

	cursor := uint16(BNODE_HEADER_LENGTH)
	for i := range n.N {
		copy(buff[cursor:], n.Keys[i])
		cursor += n.KeySize
		binary.LittleEndian.PutUint64(buff[cursor:], n.Children[i])
		cursor += CHILD_SIZE
	}
	// last child
	binary.LittleEndian.PutUint64(buff[cursor:], n.Children[n.N])
}

func (no *BTreeNode) Flush(buff []byte) error { 
	offset := int64(no.Id) * int64(pages.PAGE_SIZE)
	n, err := prims.Write(no.fd, buff, offset)
	if n < int(pages.PAGE_SIZE) {
		panic("didnt write it all")
	}
	if err != nil { return err }
	no.isDirty = false
	return nil
}

func (n *BTreeNode) init(fd int, entrySizes ...int) { 
	n.fd = fd
	n.PType = pages.IDX_NODE
	var entrySize uint16
	if len(entrySizes) > 0 {
		entrySize = uint16(entrySizes[0])
	} else {
		entrySize = n.KeySize + uint16(CHILD_SIZE)
	}
	n.Max = (pages.PAGE_SIZE - BNODE_HEADER_LENGTH - CHILD_SIZE) / uint16(entrySize)
}

func (node *BTreeNode) SplitAndInsert(q *IdxQuery, newNode *BTreeNode, key []byte, child uint64) error {
	// fmt.Printf("SPLITANODE:%d:%d ", newNode.KeySize, newNode.EntrySize)
	n := int(node.N)
	mid := n / 2
	promotedKey := node.Keys[mid]

	// Step 1: Assign keys/children to the new node
	newNode.Keys = make([][]byte, node.Max)
	newNode.Children = make([]uint64, node.Max+1)

	// Keys: from mid+1 to n-1 (excluding promoted key at mid)
	copy(newNode.Keys, node.Keys[mid+1:n])
	// Children: from mid+1 to n
	copy(newNode.Children, node.Children[mid+1:n+1])

	// Set correct N values
	newNode.N = uint16(n - mid - 1) // number of keys copied
	node.N = uint16(mid)           // keep left half only

	newNode.Lsn = node.Lsn
	node.isDirty = true
	newNode.isDirty = true

	// Step 2: Promote the middle key to the parent
	parent, err := node.GetParent(q)
	if err != nil { return err }

	if parent == nil{
		err = q.NewRootNode(node, newNode, promotedKey)
		if err != nil { return err }
	} else {
		parent.Lock()
		defer parent.Unlock()
		parent.Lsn = node.Lsn
		err = parent.AddKey(q, promotedKey, node.Id)
		if err != nil { return err }
	}

	// Step 3: Insert the new key into the correct node
	if bytes.Compare(key, promotedKey) >= 0 {
		return newNode.AddKey(q, key, child)
	}
	return node.AddKey(q, key, child)
}

func (n *BTreeNode)AddKey(q *IdxQuery, key []byte, child uint64) error {
	if n.N >= n.Max{
		// if node is full, split it
		newNode := new(BTreeNode)
		err := NewBItem(q, newNode)
		if err != nil { return err }
		newNode.Lock()
		defer newNode.Unlock()
		return n.SplitAndInsert(q, newNode, key, child)
	}

	newKeys := make([][]byte, n.N+1)
	newChildren := make([]uint64, n.N+2)
	k, i := 0, 0
	inserted := false
	for range int(n.N) {
		if !inserted && bytes.Compare(n.Keys[k], key) >= 0 {
			newKeys[i] = key
			newChildren[i] = n.Children[k]
			n.Children[k] = child
			inserted = true
			i++
		}
		newKeys[i] = n.Keys[k]
		newChildren[i] = n.Children[k]
		i++
		k++
	}
	newChildren[i] = n.Children[k]
	if !inserted {
		newKeys[i] = key
		newChildren[i+1] = child
	}

	n.Keys = newKeys
	n.Children = newChildren

	n.N++
	n.isDirty = true
	return nil
}

func (q *IdxQuery) NewRootNode(node BTreeItem, newNode BTreeItem, midKey []byte) error {
	// ACTION 1
	// log new node creation
	newId, err := q.I.Db.ClaimFreePage(pages.IDX_NODE)
	if err != nil { return err }

	axs, vs := make([]*t.Action, 1), make([]*[]byte, 1)
	axs[0], vs[0] = t.AtomicAx(t.NEWPAGE, t.Page, newId), &[]byte{}

	root, err := LoadNode(q, newId)
	if err != nil { return err }
	root.Id = newId
	root.PType = pages.IDX_NODE
	root.KeySize = uint16(q.keySize)
	root.EntrySize = uint16(q.entrySize)
	root.N = 1 // root starts with one keys
	root.init(q.fd, q.entrySize)
	root.Keys = make([][]byte, 1)
	root.Children = make([]uint64, 2)
	root.Keys[0] = midKey
	root.Children[0] =  node.GetId() // first child is the old node
	root.Children[1] =  newNode.GetId() // second child is the new node
	root.isDirty = true

	root.Lsn, err = q.I.Logger.NewTxn(&axs, &vs, q.trxId)
	start := q.I.MetaPage.Cursor + (SchemaToOrderedInt(q.schema) * 8)
	binary.LittleEndian.PutUint64(q.I.MetaPage.Body[start: start + 8], newId)

	return err
}

func (q *IdxQuery)DeleteFromParent(n *BTreeNode, key []byte) error {
	// find the key in the node
	fmt.Println("DELETED?")
	if n.N == 1 { 
		parent, err := n.GetParent(q)
		if err != nil { return err }
		parent.Lock()
		defer parent.Unlock()

		parent.Lsn = n.Lsn

		err = q.DeleteFromParent(parent, n.Keys[0])
		if err != nil { return err }

		err = q.I.Db.NewFreePage(n)
		return err
	}

	idx := -1
	for i := range int(n.N) {
		if bytes.Equal(n.Keys[i], key) {
			idx = i
			break
		}
	}
	if idx == -1 { return nil } // key not found

	// shift keys and children to delete the key
	newkeys := n.Keys[:(idx+1)*q.GetKeySize()]
	newchilds := n.Children[:(idx+1)*CHILD_SIZE]

	copy(n.Keys[idx:len(n.Keys) - 1], newkeys[idx+1:])
	copy(n.Children[idx:len(n.Children)-1 ], newchilds[idx+1:])

	// update n of the node
	n.N--
	n.isDirty = true

	return nil
}


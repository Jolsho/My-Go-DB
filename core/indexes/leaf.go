package indexes

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"mydb/core/pages"
	"mydb/core/prims"
)

const (
	NEXT_OFFSET = BNODE_HEADER_LENGTH
	PREV_OFFSET = NEXT_OFFSET 	+ 8
	DIRTY_OFFSET= PREV_OFFSET 	+ 8
	KEY_OFFSET 	= DIRTY_OFFSET 	+ 2
)

    ///////////////////////////////////////////////////////////////
   // 					LEAF NODE					 			//
  //            { header, next , prev, key, n }				   //
 //  header = {isLeaf, parentId, pageId, keySize, entrySize } //
///////////////////////////////////////////////////////////////


type BTreeLeaf struct { 
	*BTItemBase 
	Dirty 	uint16
	Next 	uint64
	Prev 	uint64
	Key 	[]byte
	IsDead 	bool
	Body 	[]byte
}

func (l *BTreeLeaf) IsLeaf() bool {return true}
func (l *BTreeLeaf) BodyOffset() int {
	// return the offset of the body in the leaf
	return KEY_OFFSET + int(l.KeySize)
}

func (l *BTreeLeaf) FromBytes(buff []byte) error {
	ItemB := new(BTItemBase)
	ItemB.fromBytes(buff)
	bodyStart := KEY_OFFSET+int(ItemB.KeySize)
	l.BTItemBase = ItemB
	l.Dirty = binary.LittleEndian.Uint16(buff[DIRTY_OFFSET:])
	l.Next = binary.LittleEndian.Uint64(buff[NEXT_OFFSET:])
	l.Prev = binary.LittleEndian.Uint64(buff[PREV_OFFSET:])
	l.Key = buff[KEY_OFFSET : bodyStart]
	l.IsDead = false
	l.Body = buff[bodyStart:]
	return nil
}

func (l *BTreeLeaf) ToBytes() []byte {
	l.BTItemBase.WriteToBuffer(l.fullBuff)
	l.WriteToBuffer(l.fullBuff)
	return l.fullBuff
}

func (l *BTreeLeaf) WriteToBuffer(buff []byte) {
	l.BTItemBase.WriteToBuffer(buff)
	binary.LittleEndian.PutUint16(buff[DIRTY_OFFSET:], l.Dirty)
	binary.LittleEndian.PutUint64(buff[NEXT_OFFSET:], l.Next)
	binary.LittleEndian.PutUint64(buff[PREV_OFFSET:], l.Prev)
	copy(buff[KEY_OFFSET:], l.Key)
	copy(buff[l.BodyOffset():], l.Body)
}

func (l *BTreeLeaf) Flush(buff []byte) error { 
	offset := int64(l.Id) * int64(pages.PAGE_SIZE)
	n, err := prims.Write(l.fd, buff, offset)
	if n < int(pages.PAGE_SIZE) {
		panic("didnt write it all")
	}
	if err != nil { return err }
	l.isDirty = false
	return nil
}

func (l *BTreeLeaf) init(fd int, entrySizes ...int) { 
	l.fd = fd
	l.Max = (((pages.PAGE_SIZE - uint16(l.BodyOffset())) / uint16(entrySizes[0])))
	l.PType = pages.IDX_LEAF
}

func (l *BTreeLeaf) PrintEntries(hasUid bool, cursors ...int) {
	isCursor := false
	cursor := 0
	if len(cursors) > 0 {
		isCursor = true
		cursor = cursors[0]
	}

	cursor1 := 0
	entrySize:= int(l.EntrySize)
	fmt.Printf("\n======================================")
	fmt.Printf("\nENTRIES")
	for cursor1 < int((l.N+l.Dirty)*uint16(entrySize)) {
		if hasUid {
			fmt.Printf("\n%v",l.Body[cursor1+16:cursor1+entrySize-8])

		} else {
			fmt.Printf("\n%v",l.Body[cursor1:cursor1+entrySize])
		}
		cursor1+=entrySize
		if isCursor && cursor1 == cursor {
			fmt.Printf("\n-- bin search cursor --")
		}
	}
	fmt.Printf("\n======================================")
}

func (l *BTreeLeaf)SearchNextSibling(q *IdxQuery) ([]int, []*BTreeLeaf){
	l.RLock()

	entries := make([]int, 0)
	if l.Next == 0 { return entries, nil }
	nextLeaf, err := LoadLeaf(q, l.Next)
	if err != nil { return entries, nil }

	l.RUnlock()


	nextLeaf.RLock()
	defer nextLeaf.RUnlock()
	siblings := make([]*BTreeLeaf, 0, 1)

	// Check if the next leaf potentially has something to offer
	if res := q.PartialCompareKey(nextLeaf.Key); res >= 0 {

		q.setKey(nextLeaf.Key)
		siblings = append(siblings, nextLeaf)

		// since we set a new key, q.compareEntry() will work
		entries, err = nextLeaf.FindMatchingEntries(q, false, true)
	}
	return entries, siblings
}



func (leaf *BTreeLeaf)Clean() (error) {
	entrySize := int(leaf.EntrySize)
	// we should clean it up instead of splitting
	// clean up the leaf by removing dirty entries

	newBody := make([]byte, int(leaf.Max) * entrySize)
	// copy only the entries that are not dirty
	var cursor, k int
	current := make([]byte, entrySize)
	for i := range int(leaf.N + leaf.Dirty)-1 {
		cursor = i * entrySize
		copy(current, leaf.Body[cursor:])

		if current[leaf.KeySize-1] != IS_DIRTY {
			copy(newBody[k * entrySize:], current)
			k++
		}
	}

	leaf.Body = newBody
	leaf.Dirty = 0 // reset dirty count
	leaf.isDirty = true
	return nil
}

func (leaf *BTreeLeaf) CleanAndInsert(q *IdxQuery, entry []byte) error {
	// fmt.Printf("CLEAN:%d ",leaf.Id)

	entrySize := int(leaf.EntrySize)
	keySize := int(leaf.KeySize)
	total := int(leaf.N + leaf.Dirty)

	read := make([]byte, entrySize)
	var writeOffset, writeIdx int
	for readIdx := range total {
		copy(read, leaf.Body[readIdx*entrySize:readIdx*entrySize+entrySize])

		// Skip dirty entries
		if read[keySize-1] == IS_DIRTY { continue }

		// Copy the current entry forward
		copy(leaf.Body[writeOffset:writeOffset+entrySize], read[:])
		writeIdx++
		writeOffset = writeIdx * entrySize
	}
	for ;writeOffset < len(leaf.Body); writeOffset++ {
		leaf.Body[writeOffset] = 0
	}

	leaf.Dirty = 0
	leaf.Insert(entry)
	return nil
}

// Split the leaf node and returns leaf which corresponds to q.key
func (leaf *BTreeLeaf)SplitAndAdd(q *IdxQuery, entry []byte) (error) {
	// load parent first
	parent, err := leaf.GetParent(q)
	if err != nil { return err }
	if parent != nil {
		nBefore := leaf.N
		leaf.Unlock()
		parent.Lock()
		defer parent.Unlock()
		leaf.Lock()

		// if the leaf was modified while we were locking the parent
		// we need to make sure we should still be splitting
		if nBefore != leaf.N {
			if nBefore / 2 == leaf.N {
				// someone else split the leaf
				// so we need to insert into the correct leaf
				if res := bytes.Compare(entry[:leaf.KeySize], leaf.Key); res > 0 {

					// we need the sibling leaf
					siblingLeaf, err := LoadLeaf(q, leaf.Next)
					if err != nil { return err }

					leaf.Unlock()
					siblingLeaf.Lock()
					defer siblingLeaf.Unlock()
					leaf.Lock()

					siblingLeaf.isDirty = true
					siblingLeaf.Lsn = leaf.Lsn
					siblingLeaf.Insert(entry)

				} else {
					// we should insert the entry into the leaf
					leaf.Insert(entry)
				}

			} else if leaf.Dirty > 0{
				// someone just removed an entry
				leaf.CleanAndInsert(q, entry)
			}
			return nil
		}
	}

	entrySize := int(leaf.EntrySize)
	halfEntryCount := int(leaf.N / 2)
	cursor := halfEntryCount * entrySize
	midEntry := make([]byte, entrySize)
	copy(midEntry, leaf.Body[cursor:cursor+entrySize])
	midKey := q.deriveKeyFromEntry(midEntry)

	// create new leaf
	newLeaf := new(BTreeLeaf)
	err = NewBItem(q, newLeaf)
	if err != nil { return err }
	newLeaf.Lock()
	defer newLeaf.Unlock()
	newLeaf.isDirty = true
	newLeaf.Dirty = 0
	newLeaf.Next = 0
	newLeaf.Prev = 0

	if parent == nil {
		err = q.NewRootNode(leaf, newLeaf, midKey)
	} else {
		parent.Lsn = leaf.Lsn
		err = parent.AddKey(q, midKey, newLeaf.Id)
	}
	if err != nil { return err }

	// split them
	copy(newLeaf.Body[:], leaf.Body[cursor:])

	newLeaf.N = uint16(halfEntryCount)
	leaf.N -= uint16(halfEntryCount)

	// fmt.Printf("SPLIT:%d:%d ", leaf.Id, newLeaf.Id)
 
	if q.compareKey(midKey) < 0 { 
		leaf.Insert(entry)
	} else { 
		newLeaf.Insert(entry) 
	}

	// inserting the new leaf into the sibling list
	leaf.isDirty = true
	newLeaf.Lsn = leaf.Lsn
	return leaf.AddLinkedList(newLeaf, q)
}

func (leaf *BTreeLeaf)Insert(entry []byte) {
	low,found := leaf.BinSearchBody(entry[:leaf.KeySize])
	if !found { 
		leaf.InsertAt(entry, low) 
	}
}

func (leaf *BTreeLeaf)InsertAt(entry []byte, low int) {
	// insert the entry at the right position
	// shift the rest of the entries to the right
	entrySize := int(leaf.EntrySize)
	cursor := int(leaf.N + leaf.Dirty) * entrySize
	for cursor >= low {
		copy(leaf.Body[cursor+entrySize:], leaf.Body[cursor:cursor+entrySize])
		if cursor == low { 
			copy(leaf.Body[cursor:], entry) 
		}
		cursor -= entrySize
	}
	leaf.N++
	leaf.isDirty = true
}

func(leaf *BTreeLeaf)DeleteEntry(i int) {
	// delete the entry at the given index
	// shift the rest of the entries to the left
	leaf.Body[i + int(leaf.KeySize-1)] = IS_DIRTY
	leaf.N--
	leaf.Dirty++
	leaf.isDirty = true
}

// NEED TO MAKE SURE PARENT IS LOCKED BEFORE CALLING THIS FUNCTION
func (leaf *BTreeLeaf)AddLinkedList(newLeaf *BTreeLeaf, q *IdxQuery) error {
	// ACTION 1
	// set prev of new leaf
	if leaf.Next != 0 {
		ol, err := LoadLeaf(q, leaf.Next) 
		if err != nil { 
			return err 
		}

		leaf.LockSibling(ol.LeafLock)
		ol.Prev = newLeaf.Id
		newLeaf.Next = ol.Id
		ol.isDirty = true
		leaf.UnlockSibling(ol.LeafLock)
	}
	newLeaf.Prev = leaf.Id
	leaf.Next = newLeaf.Id

	return nil
}

// NEED TO MAKE SURE PARENT IS LOCKED BEFORE CALLING THIS FUNCTION
func (leaf *BTreeLeaf)RemoveLinkedList(q *IdxQuery) error {
	// ACTION 1
	// set prev of next leaf
	if leaf.Next != 0 {
		other, err := LoadLeaf(q, leaf.Next)
		if err != nil { return err }
		leaf.LockSibling(other.LeafLock)
		other.Prev = leaf.Prev
		leaf.UnlockSibling(other.LeafLock)
		other.isDirty = true
	}

	// ACTION 2
	// set next of prev leaf
	if leaf.Next != 0 {
		other, err := LoadLeaf(q, leaf.Next) 
		if err != nil { return err }
		leaf.LockSibling(other.LeafLock)
		other.Next = leaf.Next
		leaf.UnlockSibling(other.LeafLock)
		other.isDirty = true
	}

	return nil
}


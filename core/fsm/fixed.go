package fsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"mydb/core/indexes"
	"mydb/core/pages"
	"mydb/core/types"
)

func (f *FSM) GetFixedSpace(size uint16, pType pages.PageType, trxId int32) (*pages.PageId, error) {
	// LOG IT
	axs := types.AtomicAx(types.GET_FIX_SPACE, types.NLBlob, 0, size)
	f.Logger.NewTxn(&[]*types.Action{axs}, &[]*[]byte{}, trxId)

	// CREATE the key we are looking for
	key := make([]byte, FSM_KEY_SIZE)
	key[0] = 1 // isFixed
	binary.LittleEndian.PutUint16(key[SIZE_OFFSET:], size) // set size
	key[COUNT_OFFSET] = 1
	key[DIRTY_OFFSET] = indexes.IS_CLEAN // dirty byte

	// Create the query struct and then find leaf for key
	q := f.NewQuery(key, 1, trxId)
	leaf, err := indexes.SearchKeys(q.IdxQuery, q.GetRoot())
	if err != nil { return nil, err }
	leaf.Lock()
	defer leaf.Unlock()

	// locate closest entry in the leaf
	cursor, found := leaf.BinSearchBody(key)
	entry := leaf.Body[cursor:cursor+FSM_ENTRY_SIZE]
	size1 := binary.LittleEndian.Uint16(entry[SIZE_OFFSET:]) // get size
	count := entry[COUNT_OFFSET] // get slot count

	pid := new(pages.PageId)
	// if the size is not equal to the requested size, we cannot use it
	// because we are looking for fixed size pages
	// so we get a new page with full slot capacity
	// we then add that to the free space map

	if size1 != size || entry[DIRTY_OFFSET] == indexes.IS_DIRTY {
		page, err := f.ClaimFreePage(pType, trxId)
		if err != nil { return nil, err }

		// create the new entry for this empty page
		newEntry := make([]byte, FSM_ENTRY_SIZE)
		newKey := make([]byte, FSM_KEY_SIZE)
		newKey[0] = 1 // isFixed
		binary.LittleEndian.PutUint16(newKey[SIZE_OFFSET:], size)
		newKey[COUNT_OFFSET] = page.GetSlotCapacity()
		newKey[DIRTY_OFFSET] = indexes.IS_CLEAN // dirty byte
		copy(newEntry[:FSM_KEY_SIZE], newKey[:]) // copy the key
		binary.LittleEndian.PutUint64(newEntry[FSM_KEY_SIZE:], page.PageId)

		if found && entry[DIRTY_OFFSET] == indexes.IS_DIRTY {
			copy(entry, newEntry)
		} else {
			if (leaf.N + leaf.Dirty) >= leaf.Max {
				if leaf.Dirty > 0 {
					leaf.CleanAndInsert(q.IdxQuery, newEntry)
				} else {
					leaf.SplitAndAdd(q.IdxQuery, newEntry)
				}
			} else {
				leaf.Insert(newEntry)
			}
		}
	
		pid.PID = uint64(page.PageId)
		pid.InPID = 0

	} else {

		// we need to update the slot count
		// if its 1 mark as dirty
		if count == 2 { 
			leaf.Dirty++
			leaf.N--
			entry[DIRTY_OFFSET] = indexes.IS_DIRTY 
			entry[COUNT_OFFSET] = count-1
		}
		// decrement the slot count
		pid.FromBytes(entry[FSM_KEY_SIZE:FSM_ENTRY_SIZE])
		entry[COUNT_OFFSET] = count-1
	}

	page, err := f.GetPage(pid)
	if err != nil { return nil, err }
	page.PageType = uint8(pType)
	page.PageId = pid.PID

	pid = page.GrabFreeSlot()
	if pid == nil { return nil, errors.New("Couldn't grab free slot") }
	page.SetIsDirty(true)
	return pid, nil
}

func (f *FSM) PutFixedSpace(size uint16, pid *pages.PageId, trxId int32) error {

	// LOGG THE THING
	axs := types.AtomicAx(types.PUT_FIX_SPACE, types.NLBlob, 0, size)
	f.Logger.NewTxn(&[]*types.Action{axs}, &[]*[]byte{}, trxId)

	key := make([]byte, FSM_KEY_SIZE)
	key[0] = 1 // isFixed
	binary.LittleEndian.PutUint16(key[SIZE_OFFSET:], size) // set size
	key[DIRTY_OFFSET] = indexes.IS_CLEAN // dirty byte

	// create a query and search for the leaf
	q := f.NewQuery(key, 1, trxId)
	leaf, err := indexes.SearchKeys(q.IdxQuery, q.GetRoot())
	if err != nil { return err }
	leaf.Lock()
	defer leaf.Unlock()

	// search for the right position to insert and copy the entry
	cursor, _ := leaf.BinSearchBody(key)
	entry := leaf.Body[cursor:cursor+FSM_ENTRY_SIZE]

	entryLocation := binary.LittleEndian.Uint64(entry[FSM_KEY_SIZE:]) // get location

	// if an entry with the right (size and destination), or is dirty, exists
	res := bytes.Compare(entry[:COUNT_OFFSET], key[:COUNT_OFFSET])
	if (res == 0 && entryLocation == pid.PID) || entry[DIRTY_OFFSET] == indexes.IS_DIRTY {
		// then the entry already exists
		if entry[DIRTY_OFFSET] == indexes.IS_DIRTY {
			// if the entry is dirty, we need to mark it as clean
			entry[DIRTY_OFFSET] = indexes.IS_CLEAN
		}

		// increment the count
		if entry[COUNT_OFFSET] == 0 {
			entry[COUNT_OFFSET] = 2
		} else {
			entry[COUNT_OFFSET] ++
		}


		// check if the location is correct
		binary.LittleEndian.PutUint64(entry[FSM_KEY_SIZE:], pid.PID)

	} else {
		// need to do the normal insertion process
		newEntry := make([]byte, FSM_ENTRY_SIZE)
		copy(newEntry, key)

		binary.LittleEndian.PutUint64(newEntry[FSM_KEY_SIZE:], pid.PID) // set location
		newEntry[COUNT_OFFSET] = 2 // set slot count to 1

		if (leaf.N + leaf.Dirty) >= leaf.Max {
			if leaf.Dirty > 0 {
				leaf.CleanAndInsert(q.IdxQuery, newEntry)
			} else {
				leaf.SplitAndAdd(q.IdxQuery, newEntry)
			}
		} else {
			leaf.Insert(newEntry)
		}
	}

	page, err := f.GetPage(pid)
	if err != nil { return err }

	page.ReleaseSlot(pid) 
	page.SetIsDirty(true)
	return nil
}

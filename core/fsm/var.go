package fsm

import (
	"encoding/binary"
	"mydb/core/indexes"
	"mydb/core/pages"
	"mydb/core/types"
)

func (f *FSM) GetVarSpace(size uint16, trxId int32) (*pages.PageId, error) {
	axs := types.AtomicAx(types.GET_VAR_SPACE, types.NLBlob, 0, size)
	f.Logger.NewTxn(&[]*types.Action{axs}, &[]*[]byte{}, trxId)

	// build the key
	key := make([]byte, FSM_KEY_SIZE)
	key[0] = 0 // !isFixed
	binary.LittleEndian.PutUint16(key[SIZE_OFFSET:], size) // set size
	key[DIRTY_OFFSET] = indexes.IS_CLEAN // dirty byte

	// create a query struct
	q := f.NewQuery(key, 1, trxId)

	// search for the correct leaf
	leaf, err := indexes.SearchKeys(q.IdxQuery, q.GetRoot())
	if err != nil { return nil, err }

	leaf.Lock()

	// find where in the leaf the possible entry is
	low, _ := leaf.BinSearchBody(key)
	entry := leaf.Body[low:low+int(leaf.EntrySize)]
	space := binary.LittleEndian.Uint16(entry[SIZE_OFFSET:]) // get size

	var page *pages.Page
	pid := &pages.PageId{}
	if space > size && entry[DIRTY_OFFSET] == indexes.IS_CLEAN {
		// mark as dirty
		entry[DIRTY_OFFSET] = indexes.IS_DIRTY
		// recover the location
		pid.FromBytes(entry[FSM_KEY_SIZE:])

		delta := space - size - pages.TUPLE_SIZE // calculate the delta size

		// load page
		page, err = f.GetPage(pid)
		if err != nil { return nil, err }

		// Now mark the tuple as alive in the tuple list of the page
		// and add inset to the pid
		err = page.GetVarSpace(pid)
		if err != nil { return nil, err }

		// Split current space which derives a new pid for leftover
		otherPid, err := page.GetLeftover(pid, size)
		if err != nil { return nil, err }

		// insert the leftover space back into the index
		// the page level update will happen in the return function call here
		newEntry := make([]byte, FSM_ENTRY_SIZE)
		copy(newEntry, entry)
		binary.LittleEndian.PutUint16(newEntry[SIZE_OFFSET:], delta)
		copy(newEntry, otherPid.ToBytes())

		q = f.NewQuery(newEntry[:FSM_KEY_SIZE], 1, trxId)
		q.InsertEntry(entry)

		leaf.Unlock()

	} else {
		leaf.Unlock()

		page, err = f.ClaimFreePage(pages.VAR_PAGE, q.GetTrxId())
		if err != nil { return nil, err }
		pid.PID = uint64(page.PageId)
		pid.InPID = 0

		// Now mark the tuple as alive in the tuple list of the page
		// and add inset to the pid
		err = page.GetVarSpace(pid)
		if err != nil { return nil, err }

		// Split current space which derives a new pid for leftover
		otherPid, err := page.GetLeftover(pid, size)
		if err != nil { return nil, err }

		// create a new entry/key for the new page
		key[0] = 0 // !isFixed
		newFreeSize := pages.PAGE_SIZE - pages.PAGE_HEADER_LENGTH - (2*pages.TUPLE_SIZE) - size
	 	binary.LittleEndian.PutUint16(key[SIZE_OFFSET:], newFreeSize)

		key[DIRTY_OFFSET] = indexes.IS_CLEAN
		copy(entry[:FSM_KEY_SIZE], key)
		copy(entry[FSM_KEY_SIZE:], otherPid.ToBytes())

		// insert this new entry, for the new page, into the leaf
		q = f.NewQuery(key, 1, trxId)
		q.InsertEntry(entry)
	}

	return pid, err
}

func (f *FSM) PutVarSpace(pid *pages.PageId, size uint16, trxId int32, isRecursed ...bool) error {
	// LOG THE THING
	axs := types.AtomicAx(types.PUT_VAR_SPACE, types.NLBlob, pid.Pack(), size)
	f.Logger.NewTxn(&[]*types.Action{axs}, &[]*[]byte{}, trxId)

	// CREATE THE KEY
	key := make([]byte, FSM_KEY_SIZE)
	key[0] = 0 // !isFixed
	binary.LittleEndian.PutUint16(key[SIZE_OFFSET:], size) // set size
	key[DIRTY_OFFSET] = indexes.IS_CLEAN // dirty byte

	// initialize the entry
	entry := make([]byte, FSM_ENTRY_SIZE)
	copy(entry[:FSM_KEY_SIZE], key[:])
	copy(entry[FSM_KEY_SIZE:], pid.ToBytes()) // set location

	// create a query
	q := f.NewQuery(key, 1, trxId)

	// JUST normal insert
	err := q.InsertEntry(entry)
	if err != nil { return err }

	// load the page
	page, err := f.GetPage(pid)
	if err != nil { return err }

	// insert new free space into page structure
	return page.PutFreeSpaceVar(pid, isRecursed...)
}

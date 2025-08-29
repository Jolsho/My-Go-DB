package indexes

import (
	"encoding/binary"
	"errors"
	"fmt"
	"mydb/core/types"
)
func (q *IdxQuery)PrintKey() {
	fmt.Printf("\n%v-MYKEY-%s\n",q.key[16:], q.schema.String())
}

func (q *IdxQuery)GetEntries(skipKey, exactMatch bool) ([]uint64,  error) {
	root := q.GetRoot()
	leaf, err := SearchKeys(q, root)
	if err != nil { return nil, err }
	leaf.RLock()
	defer leaf.RUnlock()

	insets, err :=  leaf.FindMatchingEntries(q, skipKey, exactMatch)
	if err != nil { return nil, err }

	offsets := make([]uint64, len(insets))
	if len(insets) == 0 { 
		return offsets, EntryNotFoundError 
	}

	prev := insets[0]
	for i,in := range insets {
		if in < prev {
			// means you crossed page boundary and load next leaf
			if leaf.Next != 0 {
				leaf, err = LoadLeaf(q, leaf.Next)
				if err != nil { return nil, err }
			} else { 
				break 
			}
		}
		valbytes := leaf.Body[in:in + q.GetEntrySize()]
		offsets[i] = binary.LittleEndian.Uint64(valbytes[q.GetKeySize():])
		prev = in
	}
	return offsets, nil
}

func (q *IdxQuery)InsertEntry(entry []byte) error {
	leaf, err := SearchKeys(q, q.GetRoot())
	if err != nil { return err }

	leaf.Lock()
	// Binary search the body to find the right position to insert
	low, found := leaf.BinSearchBody(q.key)
	defer leaf.Unlock()

	if !found { 
		// LOG THE INSERTION
		axs := &[]*types.Action{types.AtomicAx(types.IDX_INSERT, types.NLBlob, leaf.Id, uint16(q.entrySize))}
		leaf.Lsn, err = q.I.Logger.NewTxn(axs, &[]*[]byte{&entry}, q.trxId)
		if err != nil { return err }

		// if there are any dirty entries, we should clean it up and insert
		// the purpose is to avoid splitting
		// grab slot and check if it is full
		if (leaf.N + leaf.Dirty) == leaf.Max {
			if leaf.Dirty > 0 { 
				return leaf.CleanAndInsert(q, entry) 
			} else {
				return leaf.SplitAndAdd(q, entry)
			}
		} else {

			// if the entry below or above insertion are dirty we can just overwrite it
			if leaf.Body[low : low + q.entrySize][q.GetKeySize() - 1] == IS_DIRTY && 
			low < int((leaf.N+leaf.Dirty)*leaf.EntrySize)  {
				// overwrite the dirty entry
				copy(leaf.Body[low:low+q.entrySize], entry)
				// decrease dirty count and increase n
				leaf.Dirty--
				leaf.N++

			} else if low-q.entrySize > 0 && leaf.Body[low-q.entrySize:low][q.GetKeySize()-1] == IS_DIRTY {
				// overwrite the dirty entry
				copy(leaf.Body[low-q.entrySize:low], entry)

				// decrease dirty count and increase n
				leaf.Dirty--
				leaf.N++

			} else {
				leaf.InsertAt(entry, low)
			}
		}
		return nil
	} else {
		return errors.New("Entry Already Exists")
	}
}

func (q *IdxQuery)DeleteEntry() error {

	leaf, err := SearchKeys(q, q.GetRoot())
	if err != nil { return err }
	leaf.Lock()

    cursor, found := leaf.BinSearchBody(q.key)
    if !found { 
		leaf.Unlock()
		return nil 
	}

	// LOG THE DELETION
	axs := &[]*types.Action{types.AtomicAx(types.IDX_DELETE,types.NLBlob, leaf.Id, uint16(q.entrySize))}
	leaf.Lsn, err = q.I.Logger.NewTxn(axs, &[]*[]byte{&q.key}, q.trxId)
	if err != nil { return err }


	if leaf.N == 1 {
		fmt.Println("BIG DELL", leaf.Id, q.schema.String())
		// if the leaf is empty, remove it
		leaf.IsDead = true
		parent, err := leaf.GetParent(q)
		if err != nil {
			leaf.Unlock()
			q.I.Logger.CancelTxn(q.GetTrxId())
			return types.ErrInvalidPage
		}

		leaf.Unlock()
		parent.Lock()
		defer parent.Unlock()
		leaf.Lock()
		defer leaf.Unlock()

		parent.Lsn = leaf.Lsn

		// if someone else has deleted the leaf, we should not delete it again
		if leaf.IsDead { return nil }
		leaf.IsDead = true

		if leaf.N > 1 {
			leaf.DeleteEntry(cursor)

		} else {
			// we can safely delete the leaf
			err = q.DeleteFromParent(parent, leaf.Key)
			if err != nil {
				q.I.Logger.CancelTxn(q.GetTrxId())
				return err
			}
			leaf.RemoveLinkedList(q)
			q.freeNode(leaf)
		}

		q.I.Logger.CommitTxn(q.GetTrxId())
		return nil

	} else {
		defer leaf.Unlock()
		if leaf.IsDead { return nil }

		if leaf.Dirty > leaf.Max/2 {
			if leaf.Dirty == 0 { return nil }
			// if the leaf is half dirty, we can clean it up
			// clean up the leaf by removing dirty entries
			leaf.Clean()
		} else {
			if leaf.N <= 1 {
				// TODO need to delete the leaf here
			}

			// flip the dirty byte to indicate deletion
			leaf.DeleteEntry(cursor)
		}
		return nil
	}
}

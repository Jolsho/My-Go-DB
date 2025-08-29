package indexes

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
	"mydb/core/pages"
	"mydb/core/prims"
	"mydb/core/types"
)

func SearchKeys(q *IdxQuery, item BTreeItem) (*BTreeLeaf, error) {
	i := 0
	if node, ok := item.(*BTreeNode); ok {
		node.RLock()

		// Find the last key less than or equal to the target
		// if key is less than the first key, we are at the right page
		for i < int(node.N) && bytes.Compare(q.key, node.Keys[i]) > 0 {
			i++
		}

		// Else move to the child node which is closer to the target
		// once new node is a leaf it will return itself
		newNode, err := LoadItem(q, node.Children[i])
		node.RUnlock()
		if err != nil { 
			for i, k := range node.Keys {
				fmt.Printf("\n%v:%d ", k, node.Children[i])
			}
			fmt.Println("\nERROR:", node.Id, node.Children[i])
			return nil, err 
		}

		return SearchKeys(q, newNode)
	} else {
		return item.(*BTreeLeaf), nil
	}
}

func JustLoadNode[T BTreeItem](I *Idx, fd int, id uint64, node T) (error) {
	offset := int64(id) * int64(pages.PAGE_SIZE)

	data := I.Cache.GetBuffer()
	n, err := prims.Read(fd, data, offset)
	if n < int(pages.PAGE_SIZE) {
		panic("wronglength")
	}
	if err != nil { return err	}

	node.FromBytes(data)

	return nil
}

func LoadNode(q *IdxQuery, id uint64) (*BTreeNode, error) {
	var node *BTreeNode
	nl, ok := q.I.Cache.Get(id)
	if !ok {
		node = new(BTreeNode)
		err := JustLoadNode(q.I,q.fd, id, node)
		if err != nil { return nil, err }
		node.Id = id
		node.PType = pages.IDX_NODE

		// add the node to the cache
		node.init(q.fd, q.entrySize)
		q.I.Cache.Set(node)

	} else {
		node, ok = nl.(*BTreeNode)
		if !ok { return nil, errors.New("COULDNT CAST NODE") }
	}
	return node, nil
}

func LoadLeaf(q *IdxQuery, id uint64) (*BTreeLeaf, error) {
	var leaf *BTreeLeaf
	nl, ok := q.I.Cache.Get(id)
	if !ok {
		leaf = new(BTreeLeaf)
		err := JustLoadNode(q.I,q.fd, id, leaf)
		if err != nil { return nil, err }

		// add the node to the cache
		leaf.Id = id
		leaf.PType = pages.IDX_LEAF
		leaf.init(q.fd, q.entrySize)
		q.I.Cache.Set(leaf)
	} else {
		leaf, ok = nl.(*BTreeLeaf)
		if !ok { return nil, errors.New("COULDNT CAST LEAF") }
	}
	return leaf, nil
}

func LoadItem(q *IdxQuery, id uint64) (BTreeItem, error) {
	var i BTreeItem
	nl, ok := q.I.Cache.Get(id)
	if !ok {
		offset := int64(id) * int64(pages.PAGE_SIZE)

		data := q.I.Cache.GetBuffer()
		_, err := prims.Read(q.fd, data, offset)
		if err != nil { return nil, err	}

		if data[0] == byte(pages.IDX_LEAF) {
			i = new(BTreeLeaf)
		} else if data[0] == byte(pages.IDX_NODE){
			i = new(BTreeNode)
		} else {
			fmt.Println("loadd",time.Now().UnixMicro(), id) 
			theType := pages.PageType(data[0]).String()
			errorString := fmt.Sprintf("NOT A LEAF OR NODE:%s",theType)
			return nil, errors.New(errorString)
		}
		i.FromBytes(data)

		// add the node to the cache
		i.init(q.fd, q.entrySize)

		q.I.Cache.Set(i)
	} else {
		i, ok = nl.(BTreeItem)
		if !ok { return nil, errors.New("COULDNT CAST LEAF") }
	}
	return i, nil
}


var ( 
	EntryNotFoundError = errors.New("entry not found")
)

func (leaf *BTreeLeaf)FindMatchingEntries(
	q *IdxQuery, skipFirst, exact bool,
) ([]int, error) {

	leaf.RLock()
	defer leaf.RUnlock()

	limit := q.getLimit()
	var insets []int

	if leaf.N == 0 {
		if exact { return nil, EntryNotFoundError }
		insets = make([]int, 0)
		return insets, nil // no entries to search
	}

	var entry, key []byte
	var end int
	var res int8
	entrySize := q.GetEntrySize()

	// search for start of entry
	// if not found cursor returns closest entry > key
	cursor, found := leaf.BinSearchBody(q.key)
	if !found && exact { 
		// fmt.Printf("%d:%d ", leaf.Id, cursor)
		// if leaf.Id == 13600 && cursor == 2665{
		// 	leaf.PrintEntries(true,cursor)
		// 	q.PrintKey()
			// fmt.Println(leaf.N, leaf.Max)

		// //
		// if cursor == 5863 && leaf.Id == 1597 && leaf.N == 141 && leaf.Dirty == 2 {
		// 	fmt.Printf("\n%d:%s ", leaf.Id, q.schema.String())
		//
			// q.PrintKey()
		// 	// leaf.PrintEntries(true, cursor)
			// parent, err := leaf.GetParent(q)
			// if err != nil || parent == nil { return nil, EntryNotFoundError }
			//
			// fmt.Printf("\nparent N: %d", parent.N)
			// for i,k:=range parent.Keys {
			// 	fmt.Printf("\n%v %d",k[16:],parent.Children[i])
			// 	cursor+=entrySize
			// }
		//
		// 	// leaf, err := LoadLeaf(q,831)
		// 	// if err != nil { return nil, EntryNotFoundError }
		//
		//
		// }

		return nil, EntryNotFoundError 
	}
	lengthOFBody := int((leaf.N + leaf.Dirty) * leaf.EntrySize)

	// scan in entries
	for i := 0; i < limit; {

		if i == 0 {
			if skipFirst {
				// skip the first entry
				cursor += entrySize
			}
			end = cursor + entrySize
		}

		if end > lengthOFBody {
			// Need to search the next sibling
			q.setLimit(limit - i)
			otherEntries, siblings := leaf.SearchNextSibling(q)
			insets = append(insets, otherEntries...)
			for _, s := range siblings { 
				defer s.RUnlock()  // defer the unlock of them all
			}
			break
		}

		// slice the body to get the entry
		entry = leaf.Body[cursor:end]
		key = q.deriveKeyFromEntry(entry)

		// if the read entry is suffeciently different, break
		// EX: uid is different
		// but if say its a dead entry, we just skip and keep going (res == 1)
		// if res == 0 its a match, so increment i and assign the value
		if res = q.PartialCompareKey(key); res < 0 { 
			break 
		} else if res == 0 {
			// assign the value to the array
			insets = append(insets, cursor)
			i++
		}

		// increment the start and end offsets for next entry
		cursor += entrySize
		end = cursor + entrySize
	}
	return insets, nil
}

// if found returns cursor to entry
// else returns low position
func (l *BTreeLeaf) BinSearchBody(key []byte) (int, bool) {
	if len(l.Body) == 0 || l.N == 0 { return 0, false }

	var res, low, mid, start int
	var keyPart []byte
	entrySize := int(l.EntrySize)
	high := int(l.N + l.Dirty) - 1

	for low <= high {
		mid = (low + high) / 2
		start = mid * entrySize
		keyPart = l.Body[start : start+int(l.KeySize)]

		res = bytes.Compare(key, keyPart)
		if res < 0 {
			high = mid - 1
		} else if res > 0 {
			low = mid + 1
		} else {
			return start, true
		}
	}
	return low * entrySize, false
}

func NewBItem(q *IdxQuery, item BTreeItem) (error) {
	var pageType pages.PageType

	if item.IsLeaf() { pageType = pages.IDX_LEAF
	} else { pageType = pages.IDX_NODE }

	newId, err := q.I.Db.ClaimFreePage(pageType)
	if err != nil { return err }

	axs, vs := make([]*types.Action, 1), make([]*[]byte, 1)
	axs[0], vs[0] = types.AtomicAx(types.NEWPAGE, types.Page, newId), &[]byte{}

	lsn, err := q.I.Logger.NewTxn(&axs, &vs, q.trxId)
	if err != nil { return err }

	buff := q.I.Cache.GetBuffer()

	if item.IsLeaf() { 
		buff[PAGETYPE_OFFSET] = byte(pages.IDX_LEAF)
	} else { 
		buff[PAGETYPE_OFFSET] = byte(pages.IDX_NODE) 
	}

	binary.LittleEndian.PutUint64(buff[ID_OFFSET:], newId)
	binary.LittleEndian.PutUint16(buff[KEY_SIZE_OFFSET:], uint16(q.GetKeySize()))
	binary.LittleEndian.PutUint16(buff[ENTRY_SIZE_OFFSET:], uint16(q.GetEntrySize()))
	binary.LittleEndian.PutUint16(buff[N_OFFSET:], 0)
	binary.LittleEndian.PutUint64(buff[LSN_OFFSET:], lsn)

	item.FromBytes(buff)
	item.init(q.fd, q.entrySize)

	q.I.Cache.Set(item)

	return nil
}

package fsm

import (
	"bytes"
	"mydb/core/indexes"
	"mydb/core/logger"
	"mydb/core/pages"
	"mydb/core/types"
)

// fsm key structure = bool, size, location = PID
// bool = isFixed, size = uint16, slotCount - uint16, dirty = byte
const (
	FSM_KEY_SIZE    = 1 + 2 + 1 + 1
	FSM_ENTRY_SIZE = FSM_KEY_SIZE + 8 // key + pages.PageId

	SIZE_OFFSET =  1 // offset in the key for size
	COUNT_OFFSET = 3 // offset in the key for slot count
	DIRTY_OFFSET = 4 // offset in the key for dirty byte
)

type FSM struct { *indexes.Idx }

func NewFSM( db types.DatabaseI, l *logger.Logger,
	cache types.CacheI, metaPage *pages.Page,
) *FSM {
	idx := &FSM{}
	idx.Idx = indexes.NewIdx(db, l, cache, metaPage)
	idx.DeriveKeyFromEntry = deriveKeyFromEntry
	idx.DeriveEntryAndKey = deriveEntryAndKey
	idx.PartialCompareKey = partialCompareKey
	return idx
}

func deriveKeyFromEntry(entry []byte, _ pages.PageType) []byte { 
	return entry[:FSM_KEY_SIZE] 
}

func deriveEntryAndKey(row []byte, schema pages.PageType) ([]byte, []byte) {
	newEntry := make([]byte, FSM_ENTRY_SIZE)
	copy(newEntry[:], row[:FSM_ENTRY_SIZE])
	return newEntry, newEntry[:FSM_KEY_SIZE]
}
func partialCompareKey(qkey, key []byte, _ pages.PageType) int8 {
	ok := bytes.Equal(qkey[:COUNT_OFFSET], key[:COUNT_OFFSET])
	if !ok { return -1 }

	if key[DIRTY_OFFSET] == indexes.IS_DIRTY {
		if qkey[DIRTY_OFFSET] == indexes.IS_DIRTY { return 0 } // both dirty
		return 1 // key is dirty, query is not
	}
	return 0 // both clean
}

type fsmIdxQuery struct { *indexes.IdxQuery }

func (f *FSM) NewQuery(key []byte, limit int, trxId int32) *fsmIdxQuery {
	return &fsmIdxQuery{IdxQuery: indexes.NewIdxQuery(
			f.Idx, key, limit, FSM_ENTRY_SIZE, FSM_KEY_SIZE, pages.FSM, trxId,
	)}
}

package fileT

import (
	"bytes"
	"mydb/core/indexes"
	"mydb/core/pages"
)

const (
	// [uid] [fileType] [timestamp] [dirty] [location] => 16 + 1 + 8 + 1 = 26 + 8 = 34
	FTYPE_KEY_SIZE = 26
	FTYPE_ENTRY_SIZE = 34

	// [uid] [timestamp] [dirty] [location] => 16 + 8 + 1 = 25 + 8 = 33
	FTIME_KEY_SIZE = 25
	FTIME_ENTRY_SIZE = 33

	// [uid] [fileId] [dirty] [location] => 16 + 16 + 1 = 33 + 8 = 41
	FID_KEY_SIZE = 33
	FID_ENTRY_SIZE = 41
)

func NewFileIndex(t *FileTable, metaPage *pages.Page) *indexes.Idx {
	idx := indexes.NewIdx(t.GetDatabase(), t.GetLogger(), t.GetCache(), metaPage)
	idx.PartialCompareKey = partialCompareKey
	idx.DeriveKeyFromEntry = deriveKeyFromEntry
	idx.DeriveEntryAndKey = deriveEntryAndKey
	return idx
}

func partialCompareKey(qkey, key []byte, schema pages.PageType) int8 { 
	switch schema {
	case pages.FTYPE_IDX: 
		ok := bytes.Equal(qkey[:17], key[:17])
		if !ok { return -1 }
		if qkey[25] == key[25] { return 0 }

	case pages.FTIME_IDX:
		ok := bytes.Equal(qkey[:16], key[:16])
		if !ok { return -1 }
		if qkey[24] == key[24] { return 0 }

	// this case wont be called, ever 
	case pages.FID_IDX:
		ok := bytes.Equal(qkey[:32], key[:32])
		if !ok { return -1 }
		if qkey[32] == key[32] { return 0 }

	default: return -1
	}
	return 1
}

func deriveKeyFromEntry(entry []byte, schema pages.PageType) []byte { 
	switch schema {
	case pages.FTYPE_IDX: return entry[:FTYPE_KEY_SIZE]
	case pages.FTIME_IDX: return entry[:FTIME_KEY_SIZE]
	case pages.FID_IDX:   return entry[:FID_KEY_SIZE]
	}
	return nil
}

func deriveEntryAndKey(row []byte, schema pages.PageType) ([]byte, []byte) {
	switch schema {
	case pages.FTYPE_IDX:
		entry := make([]byte, FTYPE_ENTRY_SIZE)
		copy(entry[:16], row[:16]) 			// uid
		entry[16] = row[48] 				// filetype
		copy(entry[17:25], row[49:57]) 		// timestamp
		entry[25] = byte(indexes.IS_CLEAN) 	// clean dirty bit
		copy(entry[26:34], row[32:40]) 		// location
		return entry[:FTYPE_KEY_SIZE], entry

	case pages.FTIME_IDX:
		entry := make([]byte, FTIME_ENTRY_SIZE)
		copy(entry[:16], row[:16]) 			// uid
		copy(entry[16:24], row[49:57])		// timestamp
		entry[24] = byte(indexes.IS_CLEAN) 	// clean dirty bit
		copy(entry[25:33], row[32:40])		// location
		return entry[:FTIME_KEY_SIZE], entry

	case pages.FID_IDX:
		entry := make([]byte, FID_ENTRY_SIZE)
		copy(entry[:16], row[:16])			// uid
		copy(entry[16:32], row[16:32])		// id
		entry[32] = byte(indexes.IS_CLEAN) 	// clean dirty bit
		copy(entry[33:41], row[32:40])		// locatin
		return entry[:FID_KEY_SIZE], entry

	}
	return nil, nil
}


func NewFTypeQuery(
	i *indexes.Idx, key []byte, 
	limit int, trxId ...int32,
) *indexes.IdxQuery {

	if len(trxId) == 0 { trxId = append(trxId, 0) }
	return indexes.NewIdxQuery( 
		i, key, limit, 
		FTYPE_ENTRY_SIZE, FTYPE_KEY_SIZE, 
		pages.FTYPE_IDX, trxId[0],
	)
}

func NewFTimeQuery(
	i *indexes.Idx, key []byte, 
	limit int, trxId ...int32,
) *indexes.IdxQuery {

	if len(trxId) == 0 { trxId = append(trxId, 0) }
	return indexes.NewIdxQuery( 
		i, key, limit, 
		FTIME_ENTRY_SIZE, FTIME_KEY_SIZE, 
		pages.FTIME_IDX, trxId[0],
	)
}
func NewFIDQuery(
	i *indexes.Idx, key []byte, trxId ...int32,
) *indexes.IdxQuery {
	if len(trxId) == 0 { trxId = append(trxId, 0) }
	return indexes.NewIdxQuery( 
		i, key, 1, 
		FID_ENTRY_SIZE, FID_KEY_SIZE, 
		pages.FID_IDX, trxId[0],
	)
}

func NewFileEntry(i *indexes.Idx,row []byte, trxId int32) (error) {
	var key []byte
	var entry []byte

	q := NewFTypeQuery(i,key, 0, trxId)
	entry = q.DeriveEntryAndKey(row)
	err := q.InsertEntry(entry)
	if err != nil { return err }

	q = NewFTimeQuery(i,key, 0, trxId)
	entry = q.DeriveEntryAndKey(row)
	err = q.InsertEntry(entry)
	if err != nil { return err }

	q = NewFIDQuery(i,key, trxId)
	entry = q.DeriveEntryAndKey(row)
	err = q.InsertEntry(entry)

	return err
}

func DeleteFileEntry(i *indexes.Idx, row []byte, trxId int32) (error) {
	var key []byte

	q := NewFTypeQuery(i, key, 1, trxId)
	q.DeriveEntryAndKey(row)
	err := q.DeleteEntry()
	if err != nil { return err }

	q = NewFTimeQuery(i, key, 1, trxId)
	q.DeriveEntryAndKey(row)
	err = q.DeleteEntry()
	if err != nil { return err }

	q = NewFIDQuery(i, key, trxId)
	q.DeriveEntryAndKey(row)
	err = q.DeleteEntry()

	return err
}

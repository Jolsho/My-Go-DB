package indexes

import (
	"bytes"
	"encoding/binary"
	"mydb/core/logger"
	"mydb/core/pages"
	"mydb/core/prims"
	"mydb/core/types"
)

const (
	IS_CLEAN = 0
	IS_DIRTY = 1
)


type Idx struct {
	Logger *logger.Logger
	Db types.DatabaseI
	Cache types.CacheI
	Fd int
	MetaPage *pages.Page
	metaCursor uint16 // cursor for the meta page
	PartialCompareKey func(qkey, key []byte, schema pages.PageType) int8
	DeriveKeyFromEntry func(entry []byte, schema pages.PageType) []byte
	DeriveEntryAndKey func(row []byte, schema pages.PageType) ([]byte, []byte)
}

// Still need to assign the function pointers
// for PartialCompareKey, DeriveKeyFromEntry, DeriveEntryAndKey
func NewIdx(d types.DatabaseI, l *logger.Logger, cache types.CacheI, meta *pages.Page) *Idx {
	idx := new(Idx)
	idx.Fd = d.GetFd()
	idx.Db = d
	idx.Cache = cache
	idx.Logger = l
	idx.MetaPage = meta
	idx.metaCursor = meta.Cursor // copy this to avoid being modded
	return idx
}

type IdxQuery struct {
	I *Idx
	fd int
	trxId int32
	key []byte
	limit int
	entrySize int
	keySize int
	schema pages.PageType
}

func NewIdxQuery(
	i *Idx, key []byte, 
	limit, entrySize, keySize int, 
	schema pages.PageType, trxId int32,
) *IdxQuery {
	return &IdxQuery{ i, i.Fd, trxId, key, limit, entrySize, keySize, schema}
}

func (q *IdxQuery) GetTrxId() int32 { return q.trxId }
func (q *IdxQuery) SetTrxId(id int32) { q.trxId = id }
func (q *IdxQuery) GetEntrySize() int { return q.entrySize }
func (q *IdxQuery) getLimit() int { return q.limit }
func (q *IdxQuery) setLimit(limit int) { q.limit = limit }

func (q *IdxQuery) GetKey() []byte { return q.key }
func (q *IdxQuery) setKey(key []byte) { q.key = key }
func (q *IdxQuery) GetKeySize() int { return q.keySize }
func (q *IdxQuery) compareKey(key []byte) int { return bytes.Compare(q.key[:], key[:]) }
func (q *IdxQuery) PartialCompareKey(key []byte) int8 { return q.I.PartialCompareKey(q.key, key, q.schema) }
func (q *IdxQuery) deriveKeyFromEntry(entry []byte) []byte { return q.I.DeriveKeyFromEntry(entry, q.schema) }
func (q *IdxQuery) DeriveEntryAndKey(row []byte) (entry []byte) { 
	// DeriveEntryAndKey extracts the entry and key from the row.
	// and sets the key in the query object.
	// returns the entry.
	key, entry := q.I.DeriveEntryAndKey(row, q.schema) 
	q.key = key
	return entry
}

func SchemaToOrderedInt(schema pages.PageType) uint16 {
	switch schema {
	case pages.LOGGER_PAGE: return 0
	case pages.FSM: return 1
	
	case pages.FTYPE_IDX: return 2
	case pages.FTIME_IDX: return 3
	case pages.FID_IDX: return 4

	default: return 7 // invalid schema
	}
}

func (q *IdxQuery) freeNode(p BTreeItem) error { 
	if p == nil { return nil }
	axs, vs := make([]*types.Action, 1), make([]*[]byte, 1)
	axs[0], vs[0] = types.AtomicAx(types.DELETE, types.Page, p.GetId()), &[]byte{}
	lsn, err := q.I.Logger.NewTxn(&axs, &vs, q.GetTrxId())
	if err != nil { return err }
	p.SetLsn(lsn)
	return nil
}

func (q *IdxQuery) GetRoot() BTreeItem { 
	start := q.I.MetaPage.Cursor + (SchemaToOrderedInt(q.schema) * 8)
	byt := q.I.MetaPage.Body[start: start + 8]

	rootId := binary.LittleEndian.Uint64(byt)

	var root BTreeItem
	if rootId != 0 {
		pl, _:= q.I.Cache.Get(rootId)
		if pl != nil { root = pl.(BTreeItem) }
	}

	if rootId == 0 {
		root = new(BTreeLeaf)
		err := NewBItem(q, root)
		if err != nil { return nil }

		binary.LittleEndian.PutUint64(q.I.MetaPage.Body[start:start+8], root.GetId())

	} else if root == nil {

		buff := q.I.Cache.GetBuffer()
		_, err := prims.Read(q.fd, buff, int64(rootId) * int64(pages.PAGE_SIZE))
		if err != nil { return nil }

		var root BTreeItem
		if buff[0] == byte(pages.IDX_LEAF) {
			root = new(BTreeLeaf)
		} else {
			root = new(BTreeNode)
		}
		root.init(q.fd, q.entrySize)
		root.FromBytes(buff)

		q.I.Cache.Set(root)
	}

	return root
}


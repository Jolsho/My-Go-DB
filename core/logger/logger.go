package logger

import (
	"encoding/binary"
	"mydb/core/pages"
	"mydb/core/types"
)

var (
	LOGGER_HEADER_SIZE = pages.PAGE_SIZE
)
const (
	// once this is hit we free up the log space
	LogThreshold = 1024 * 1024 * 40 // 40MB

	// add 22 bytes as padding so logger slots are easier to manage
	// length, trxId, begin, commit.
	TxHeaderLength = 4 + 4 + 1 + 1 + 22 
)

type Logger struct {
	HeaderId uint64
	Fd int
	RecoverChan chan *[]*types.Action
	Lsn uint64

	WriterIn chan *Record
	WriterOut chan error

	pendingTrxs map[int32][]*types.Action
	GetPageLike func(uint64) (types.PageLike, error)

	db types.DatabaseI
	Page *pages.Page

	Offset uint16
	ByteCount uint32
	oldest uint64
	oldestCursor uint16

	buff []byte
}

// LOGGER DOESNT NEED A FREE PAGE LIST
// BECAUSE YOU DELETE PAGES WHOLESALE
// YOU DONT DELETE INDIVIDUAL ENTRIES
func StartLogger( 
	db types.DatabaseI, metaPage *pages.Page, 
	getPLike func(uint64) (types.PageLike, error),
) (*Logger, error) {
	l := &Logger{
		Fd: db.GetFd(), 
		db: db,
		GetPageLike: getPLike,
		Page: nil, Offset: 0, ByteCount: 0,
		oldest: 0, oldestCursor: 0,
		WriterIn: make(chan *Record, 1),
		WriterOut: make(chan error, 1),
		RecoverChan: make(chan *[]*types.Action, 1),
		pendingTrxs: make(map[int32][]*types.Action),
		buff: make([]byte, pages.PAGE_SIZE),
	}
	var err error
	id := binary.LittleEndian.Uint64(metaPage.Body[metaPage.Cursor:])
	if id <= 0 {
		id, err = db.ClaimFreePage(pages.LOGGER_PAGE)
		if err != nil { return nil, err }
		binary.LittleEndian.PutUint32(metaPage.Body[metaPage.Cursor:], uint32(id))
	}
	metaPage.Cursor += 4 // pass through the pageId
	l.Offset = binary.LittleEndian.Uint16(metaPage.Body[metaPage.Cursor:])
	metaPage.Cursor += 2 // pass through the offset
	l.ByteCount = binary.LittleEndian.Uint32(metaPage.Body[metaPage.Cursor:])
	metaPage.Cursor += 4 // pass through the byteCount
	l.oldest = binary.LittleEndian.Uint64(metaPage.Body[metaPage.Cursor:])
	metaPage.Cursor += 4 // pass through the oldest pageId
	l.oldestCursor = binary.LittleEndian.Uint16(metaPage.Body[metaPage.Cursor:])
	metaPage.Cursor += 2 // pass through the oldest cursor

	// Load into the logger
	buff := make([]byte, pages.PAGE_SIZE)
	l.Page, err = pages.LoadPage(l.Fd, id, buff)
	if err != nil { return nil, err }

	if l.oldest != 0 { l.StartupRecovery()
	} else { l.oldest = l.Page.PageId }

	go l.StartWriter()
	return l, nil
}

func (l *Logger) Close() {
	buff := make([]byte, pages.PAGE_SIZE)
	meta, err := pages.LoadPage(l.Fd, l.HeaderId, buff)
	if err != nil { return }
	// Write current info to the meta page
	binary.LittleEndian.PutUint16(meta.Body[0:2], l.Offset)
	binary.LittleEndian.PutUint32(meta.Body[2:6], l.ByteCount)
	binary.LittleEndian.PutUint32(meta.Body[6:10], uint32(l.Page.PageId))
	binary.LittleEndian.PutUint32(meta.Body[10:14], uint32(l.oldest))
	binary.LittleEndian.PutUint16(meta.Body[14:16], l.oldestCursor)
}


func (l *Logger) NewPage() (*pages.Page, error) {
	pageId, err := l.db.ClaimFreePage(pages.LOGGER_PAGE)
	if err != nil { return nil, err }

	newPage, err := pages.LoadPage(l.Fd, pageId, l.buff)
	if err != nil { return nil, err }

	// point current page to the next
	l.Page.Next = pageId
	l.Offset = 0

	return newPage, nil
}

func (l *Logger) NewFreePage(p types.PageLike) error {
	return l.db.NewFreePage(p)
}

func (l *Logger) NextLsn() uint64 {
	l.Lsn++
	return l.Lsn
}

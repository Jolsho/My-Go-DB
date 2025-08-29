package logger

import (
	"encoding/binary"
	"mydb/core/types"
)

func (l *Logger) StartWriter() {
	for r := range l.WriterIn {
		if r == nil { break }
		err := l.WriteTrx(r.action, r.value, r.id, r.commitCode)
		l.WriterOut <- err
	}
}

type Record struct {
	action *types.Action
	value  *[]byte
	id     int32
	commitCode types.LogFlag
}

func (l *Logger) ToWriter (
	a *types.Action, v *[]byte, id int32, commitCode types.LogFlag,
) error {
	r := &Record{action:a, value:v, id:id, commitCode: commitCode}
	l.WriterIn <- r
	err := <-l.WriterOut
	return err
}

func (l *Logger) WriteTrx (
	a *types.Action, v *[]byte, id int32, commitCode types.LogFlag,
) error {
	var err error
	var wrote uint32
	pageSize := uint16(len(l.Page.Body))

	// MAKE sure the trx card can fit
	if l.Offset + TRX_SIZE > pageSize {
		l.Page, err = l.NewPage()
		if err != nil { return err }
	}
	aPage := l.Page
	aCursor := l.Offset
	l.Offset += TRX_SIZE
	a.Offset = l.Offset
	a.PageId = l.Page.PageId

	// write the value first to ensure recovery works
	for _, b := range *v {
		if l.Offset >= pageSize {
			l.Page, err = l.NewPage()
			if err != nil { return err }
		}
		l.Page.Body[l.Offset] = b
		l.Offset++
		wrote++
	}

	// write begin flag
	aPage.Body[aCursor] = byte(types.TxnBegin)
	aCursor++
	wrote++

	// write the transaction id
	binary.LittleEndian.PutUint32(aPage.Body[aCursor:], uint32(id))
	wrote += 4
	aCursor += 4

	// write the action
	for _, b := range a.GetRaw() {
		aPage.Body[aCursor] = b
		aCursor++
		wrote++
	}

	// write the commit commitCode
	aPage.Body[aCursor] = byte(commitCode)
	aCursor++
	wrote++

	// update byte count
	l.ByteCount += wrote
	if int(l.ByteCount) >= LogThreshold {
		go l.TruncateLogs()
	}
	return nil
}

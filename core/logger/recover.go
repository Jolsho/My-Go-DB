package logger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"mydb/core/pages"
	"mydb/core/types"
)

// TODO -- I thought since pages are locked on operation
// and actions could rely on another uncommited one and that would cause issue
// BUT I think we are good
// because the actions are suffeciently generic where we can just replay them
// so even if an insertion, prior to corruption, relys on an uncommitted state
// we can still just replay the action and derive a different state


const TRX_SIZE = types.ACTION_SIZE + 2 + 4 // + comit flags + 4 bytes for trxId


func (l *Logger) StartupRecovery() {
	p, err := pages.LoadPage(l.Fd, l.oldest, l.buff)
	if err != nil { return }

	cursor := int(l.oldestCursor)
	pending := make(map[int32][]*types.Action)
	var next uint64
	defer close(l.RecoverChan)

	for {
		if cursor + TRX_SIZE >= len(p.Body) {
			// no more actions in this Page
			if p.Next == 0 { break }
			cursor = cursor % len(p.Body)
			// load the next Page
			p, err = pages.LoadPage(l.Fd, next, l.buff)
			if err != nil { return }
		}

		// read the action
		trxId, action, commitFlag, err := l.ReadAction(p.GetId(), &cursor)
		if err != nil { return }

		switch commitFlag {
			case types.TxnCommit:
				// commit the action
				if acts, ok := pending[trxId]; ok {
					l.RecoverChan <- &acts
					delete(pending, trxId)
				} else {
					// commit the action
					acts := []*types.Action{action}
					l.RecoverChan <- &acts
				}
			case types.TxnPending:
				// add to pending list
				if _, ok := pending[trxId]; !ok {
					acts := []*types.Action{action}
					pending[trxId] = acts
				}
				pending[trxId] = append(pending[trxId], action)

			default: return
		}
	}
	for _, acts := range pending {
		// these are all pending actions that didnt get committed
		for _, act := range acts {
			// if we took a new page, we need to return it
			if act.GetOperation() == types.NEWPAGE {
				// return the page to the database
				page, err := pages.LoadPage(l.Fd, uint64(act.GetDest())/ uint64(pages.PAGE_SIZE), l.buff)
				if err != nil { return }
				if err = l.db.NewFreePage(page); err != nil { return }
			}
			// otherwise I dont think there is anything to do here
		}
	}
}


func (l *Logger) TruncateLogs() {
	// this skips ... begin flag, opcode, destOffset, vtype, vlength
	fmt.Println("CALLED TRUNCATE LOGS")

	var length uint16
	var next uint64
	cursor := int(l.oldestCursor)
	for l.ByteCount > LogThreshold/4 {
		page, err := pages.LoadPage(l.Fd, l.oldest, l.buff)
		if err != nil { return }

		for {
			cursor += TRX_SIZE
			if cursor <= len(page.Body) { break }

			length = binary.LittleEndian.Uint16(page.Body[cursor-3:cursor])

			cursor += int(length)

			if cursor >= len(page.Body) { break }
		}

		l.ByteCount -= uint32(cursor - (cursor % int(pages.PAGE_SIZE)))
		cursor = cursor % int(pages.PAGE_SIZE)

		if page.Next == 0 { break }
		l.oldest = next
		l.oldestCursor = uint16(cursor)

		if err := l.db.NewFreePage(page); err != nil { return }
	}
}

func (l *Logger) ReadAction(pageId uint64, c *int) (int32, *types.Action, types.LogFlag, error) {
	p, err := pages.LoadPage(l.Fd, pageId, l.buff)
	if err != nil { return 0, nil, 0, err }
	cursor := *c

	// read begin flag
	if p.Body[cursor] != byte(types.TxnBegin) {
		return 0, nil, 0, errors.New("Invalid begin flag")
	}
	cursor++

	// read trxID
	trxId := int32(binary.LittleEndian.Uint32(p.Body[cursor:cursor+4]))
	if trxId == 0 { return 0, nil, 0, errors.New("Invalid trxId") }
	cursor += 4

	// read action body
	action := &types.Action{}
	action.SetRaw(p.Body[cursor:cursor+types.ACTION_SIZE])
	cursor += types.ACTION_SIZE

	// Check for invalid action
	if err = action.Validate(); err != nil { return 0, nil, 0, err }

	// read the commit flag
	commitFlag := types.LogFlag(p.Body[cursor])
	cursor++

	// skip the length of the value
	cursor += int(action.GetVLength())

	return trxId, action, commitFlag, nil
}

func (l *Logger) GetValue(a *types.Action) []byte {
	var err error
	page := l.Page
	val := make([]byte, a.GetVLength())
	read := 0
	toGo := int(a.GetVLength())
	cursor := int(a.Offset)

	for read < toGo {
		if cursor+toGo > len(page.Body) {
			copyLen := len(page.Body) - cursor
			copy(val[read:read+copyLen], page.Body[cursor:cursor+copyLen])
			read += copyLen
			toGo -= copyLen
			page, err = pages.LoadPage(l.Fd, page.Next, l.buff)
			if err != nil { return nil }
			cursor = 0
		} else {
			copy(val[read:read+toGo], page.Body[cursor:cursor+toGo])
			read += toGo
			break
		}
	}
	return val
}

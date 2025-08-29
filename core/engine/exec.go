package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"mydb/core/logger"
	"mydb/core/pages"
	"mydb/core/types"
)

func ExecuteAction(l *logger.Logger, a *types.Action, p types.PageLike) error {
	switch a.GetOperation() {
		case types.INSERT, types.UPDATE: return Insert(l,a, p)
		case types.DELETE: return Delete(l, a, p)
		case types.NONE, types.NEWPAGE: return nil
		default: return errors.New("Unknown action operation")
	}
}

func ExecuteTrx(l *logger.Logger, trxId int32 )error {
	actions := l.GetActions(trxId)
	if actions == nil || len(*actions) == 0 {
		return errors.New("No actions found for transaction")
	}
	fmt.Printf("TRX: %d, AXSLen: %d\n", trxId, len(*actions))
	var err error
	ps := make(map[int32]types.PageLike)
	for _, action := range *actions {
		pageId := int32(action.GetDest()/int64(pages.PAGE_SIZE))
		if _, ok := ps[pageId]; !ok {
			tablePage, err := l.GetPageLike(uint64(action.GetDest())/uint64(pages.PAGE_SIZE))
			if err != nil { return err }
			tablePage.Lock()
			ps[pageId] = tablePage
		}

		ExecuteAction(l, action, ps[pageId])
	}

	for _, p := range ps { p.Unlock() }

	return err
}

func Insert(l *logger.Logger, action *types.Action, p types.PageLike) error {
	// Load in table page
	cursor := uint16(action.GetDest() % int64(pages.PAGE_SIZE))
	dest := p.ToBytes()

	vType := types.DataType(action.GetVType())
	if vType == types.ChainBlob {
		// Write a zeroed int64 that can be used to connect blobs
		placeHolder := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		copy(dest[cursor:], placeHolder)
		cursor += 8
	}

	if vType == types.Blob || vType == types.String {
		// Write the variable length value
		lengthBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(lengthBytes, action.GetVLength())
		copy(dest[cursor:], lengthBytes)
		cursor += 2
	}

	if action.GetVLength() == 0 { 
		fmt.Println(action.GetVType())
		return nil 
	}

	copy(dest[cursor:], l.GetValue(action))
	return nil
}

// Only handles the deletion of pages.
// because otherwise deletion is just inserting/updating the FSM
// but with pages we have to send the page to the database struct
func Delete(l *logger.Logger, action *types.Action, p types.PageLike) error {
	if types.DataType(action.GetVType()) == types.Page {
		pageId := uint64(action.GetDest()) / uint64(pages.PAGE_SIZE)
		p, err := l.GetPageLike(pageId)
		if err != nil { return err }

		return l.NewFreePage(p)
	}
	return nil
}

package logger

import (
	"mydb/core/types"
	utils "mydb/utils"
)

// Writes the value, and records offset.
// The offset is stored in the action.
// Transacton is derived and added to log table.
// Optional commitcode is used for pending transactions.
// If commitCode is not provided it defaults to at end of actions.
func (l *Logger) NewTxn(as *[]*types.Action, vs *[]*[]byte, id int32, commitCode ...types.LogFlag) (uint64, error) {
	if as == nil || len(*as) < 1 { return 0, nil }
	if id == 0 { 
		var err error 
		id, err = utils.RandomInt32() 
		if err != nil { return 0,err }
	}
	var err error

	actions := *as
	// Write the values to the logger
	// Record the offsets
	code := types.TxnPending
	isCode := len(commitCode) > 0
	for i, v := range *vs {
		if isCode && i == len(*vs) - 1 {
			code = commitCode[0]
		}

		if actions[i].GetVType() == int8(types.NLBlob) {
			actions[i].SetVLength(uint16(len(*v)))
		}
		// write this value to a page that doesnt use bitmap
		err = l.ToWriter(actions[i], v, id, code)
		if err != nil { return 0,err }
	}
	if _, ok := l.pendingTrxs[id]; ok {
		l.pendingTrxs[id] = append(l.pendingTrxs[id], *as...)
	} else {
		l.pendingTrxs[id] = *as
	}
	return l.NextLsn(), nil
}

func (l *Logger) CancelTxn(trxId int32) {
	if trxId == 0 { return }
	if _, ok := l.pendingTrxs[trxId]; ok {
		delete(l.pendingTrxs, trxId)
		axs := types.AtomicAx(types.CANCEL, types.Nil, 0)
		value := make([]byte, 0)
		l.ToWriter(axs, &value, trxId, types.TxnCancel)
	}
}

func (l *Logger) CommitTxn(trxId int32) {
	if trxId == 0 { return }
	if _, ok := l.pendingTrxs[trxId]; ok {
		axs, va := make([]*types.Action, 1), make([]*[]byte, 1)
		axs[0] = types.AtomicAx(types.NONE, types.Nil, 0)
		va[0] = &[]byte{}
		l.NewTxn(&axs, &va,trxId, types.TxnCommit)
	}
}

func (l *Logger) GetActions(trxId int32) *[]*types.Action {
	if trxId == 0 { return nil }
	axs, ok := l.pendingTrxs[trxId]
	if !ok { return nil }
	delete(l.pendingTrxs, trxId)
	return &axs
}

func (l *Logger) SnapPageLike(p types.PageLike) error {
	buff := p.ToBytes()
	pid := p.GetId()
	if buff == nil { return types.ErrInvalidPage }

	axs := make([]*types.Action, 1)
	axs[0] = types.AtomicAx(types.SNAPSHOT, types.Page, pid)
	coppy := make([]byte, len(buff))
	copy(coppy, buff)

	id, err := utils.RandomInt32()
	if err != nil { return err }

	l.NewTxn(&axs, &[]*[]byte{&coppy}, id)
	l.CommitTxn(id)
	return nil
}

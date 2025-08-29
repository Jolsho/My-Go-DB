package types

import (
	"encoding/binary"
	"errors"
)

const (
	// operation, dest, vType, vLength
	ACTION_SIZE = 1 + 1 + 8 + 1 + 2 

	// ACTION OFFSETS
	OperationOffset = 0
	DestOffset = 1
	VTypeOffset = 9
	VLengthOffset = 10
)

type Action struct {
	Offset uint16
	PageId uint64
	body []byte
}

// Creates the action to be passed to the logger,
// and then to the execution engine.
// If you want to chain trxs together add trxId.
// and pass the pending commit flag with this to NewTxn
func InsertAx( vType DataType, pid uint64, length ...uint16) (*Action) {
	return AtomicAx(INSERT, vType, pid, length...)
}
func DeleteAx( vType DataType, pid uint64, length ...uint16) (*Action) {
	return AtomicAx(DELETE, vType, pid, length...)
}
func UpdateAx( vType DataType, pid uint64, length ...uint16) (*Action) {
	return AtomicAx(UPDATE, vType, pid, length...)
}

func AtomicAx(
	OpCode OpCode, vType DataType, pid uint64, length ...uint16,
) (*Action) {
	a := &Action{ body: make([]byte, ACTION_SIZE), }
	a.SetOperation(OpCode)
	a.SetDest(pid)
	a.SetVType(int8(vType))
	if len(length) == 0 { a.SetVLength(uint16(DetermineLength(vType)))
	} else { a.SetVLength(length[0]) }
	return a
}

func (a *Action) GetRaw() []byte { return a.body }
func (a *Action) SetRaw(buff []byte) {
	if len(buff) != ACTION_SIZE {
		panic("Invalid action size")
	}
	a.body = buff
}
func (a *Action) GetOperation() OpCode { return OpCode(a.body[OperationOffset]) }
func (a *Action) SetOperation(op OpCode) { a.body[OperationOffset] = byte(op) }
func (a *Action) GetDest() int64 {
	return int64(binary.LittleEndian.Uint64(a.body[DestOffset:]))
}
func (a *Action) SetDest(dest uint64) {
	binary.LittleEndian.PutUint64(a.body[DestOffset:], dest)
}
func (a *Action) GetVType() int8 { return int8(a.body[VTypeOffset]) }
func (a *Action) SetVType(vType int8) { a.body[VTypeOffset] = byte(vType) }
func (a *Action) GetVLength() uint16 {
	return binary.LittleEndian.Uint16(a.body[VLengthOffset:])
}
func (a *Action) SetVLength(length uint16) {
	binary.LittleEndian.PutUint16(a.body[VLengthOffset:], length)
}
func (a *Action) Validate() error {
	if a.body == nil || len(a.body) != ACTION_SIZE {
		return errors.New("Invalid action")
	}
	if a.GetOperation() < 0 || a.GetOperation() > 3 {
		return errors.New("Invalid operation")
	}
	if a.GetDest() < 0 || a.GetDest() == 0 {
		return errors.New("Invalid dest")
	}
	if a.GetVLength() == 0 {
		return errors.New("Invalid vLength")
	}
	return nil
}

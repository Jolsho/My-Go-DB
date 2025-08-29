package pages

import (
	"encoding/binary"
)

type PageId struct {
	PID 	uint64
	InPID	uint8 	//inner page id
	Inset 	uint16
}

func NewPageId(pid uint64) *PageId {
	// Extract the last byte (least significant byte as INPID)
	array := make([]byte, 8)
	binary.LittleEndian.PutUint64(array, pid)
	inpid := array[7]
	array[7] = 0

	return &PageId{
		PID: binary.LittleEndian.Uint64(array),
		InPID: inpid,
	}
}

func (i *PageId) ToBytes() []byte {
    // Convert PID to 8-byte buffer, then copy only the lower 5 bytes
    buff := make([]byte, 8)
    binary.LittleEndian.PutUint64(buff, i.PID)
	buff[7] = i.InPID
	return buff
}

func (i *PageId) FromBytes(data []byte) {
	// Extract the last byte (least significant byte as INPID)
	i.InPID = uint8(data[7])
	data[7] = 0

	// Zero out the last byte in the PID
	i.PID = binary.LittleEndian.Uint64(data)
}

func (i *PageId) Pack() uint64 {
	return (i.PID &^ (uint64(0xFF)<<56)) | (uint64(i.InPID)<<56)
}

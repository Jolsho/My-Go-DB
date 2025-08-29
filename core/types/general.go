package types

import (
	"encoding/binary"
	"errors"
	"mydb/core/pages"
)

type CacheI interface {
	Get(uint64) (PageLike, bool)
	Set(PageLike)
	GetBuffer() []byte
	PutBuffer([]byte)
}

type DatabaseI interface {
	Start(string) error
	Close() error
	ClaimFreePage(pages.PageType) (uint64, error)
	NewFreePage(PageLike) error
	GetFd() int
}

type PageLike interface {
	GetId() uint64
	GetType() pages.PageType
	ToBytes() []byte
	FromBytes([]byte) error
	Flush([]byte) error
	IsDirty() bool
	SetIsDirty(bool)

	RLock()
	RUnlock()
	Lock()
	Unlock()

	InUse() bool
	ToggleInUse()
}

type DataType int8

const (
	// Fixed length types
	Int64 DataType = iota
	Int32
	Int16
	Int8
	Float
	Bool
	Time
	UUID
	Hash
	Nil
	Page

	// Variable length types
	String
	Blob
	NLBlob //no length blob
	ChainBlob // blob with int64 pointer to next blob

	// Variable length types for padding
	FileRowPadding
)
func DetermineLength(t DataType) int {
	switch t {
	case Int64: return 8
	case Int32: return 4
	case Int16: return 2
	case Int8: return 1
	case Float: return 4
	case Bool: return 1
	case Time: return 8
	case UUID: return 16
	case Hash: return 16
	case Nil: return 0
	case Page: return 0
	default: return 0
	}
}

var (
	Direction = binary.LittleEndian

	RowNotFound = errors.New("Row not found")
	AlreadyExists = errors.New("Already exists")
	TypeNotSupported = errors.New("Type not supported")
	InvalidOffset = errors.New("Invalid offset")
	InvalidUUID = errors.New("Invalid UUID length")
	SchemaNotFound = errors.New("Schema not found")
	ErrInvalidPage = errors.New("Invalid page type")
)

type DbMessage struct {
	Code 	TableCode
	Msg 	any
	Output 	chan any
}

type TableCode int8
const (
	// FILE TABLE
	FILE_TABLE TableCode = iota
	GET_FILE
	INSERT_FILE
	DELETE_FILE


	// META TABLE
	TERMINATE
	RECOVERY
)


type OpCode int8
const(
	// OPERATION CODES
	INSERT OpCode = iota
	DELETE
	UPDATE

	GET_VAR_SPACE
	PUT_VAR_SPACE

	GET_FIX_SPACE
	PUT_FIX_SPACE

	IDX_INSERT
	IDX_DELETE
	IDX_UPDATE

	NEWPAGE
	SNAPSHOT
	CANCEL
	NONE
)

type LogFlag int8
const (
	// COMMIT FLAGS
	TxnBegin 	LogFlag = 120
	TxnPending 	LogFlag = 121
	TxnCommit 	LogFlag = 122
	TxnCancel 	LogFlag = 123
)

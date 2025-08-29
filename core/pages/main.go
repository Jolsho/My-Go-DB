package pages

import (
	"encoding/binary"
	"sync"
	"syscall"
	"mydb/core/prims"
)

const (
	PAGE_HEADER_LENGTH uint16 = 1 + 4 + 2 + 2 + 4 + 8 + 1
	PAGETYPE_OFF = 0
	PAGEID_OFF = 1
	NEXT_OFF = 9
	PREV_OFF = 17
	LSN_OFF = 25
	PAGE_SIZE uint16 = 4096
	PAGE_BODY_SIZE = PAGE_SIZE - PAGE_HEADER_LENGTH
)

type Page struct {
	fd         	int
	isDirty		bool
	Lsn			uint64
	TupleLen	uint8

	PageType 	uint8
	PageId 		uint64

	Next 		uint64
	Prev 		uint64

	Body   		[]byte
	Cursor 		uint16
	fullBuffer 	[]byte
	inUse		bool

	lock 	*sync.RWMutex
}
func (p *Page) GetId() uint64 { return p.PageId }
func (p *Page) GetType() PageType { return PageType(p.PageType) }

func (p *Page) IsDirty() bool { return p.isDirty }
func (p *Page) SetIsDirty(is bool) { p.isDirty = is }

func (p *Page) Lock(){ 
	p.ToggleInUse()
	p.lock.Lock()
}
func (p *Page) Unlock(){ 
	p.ToggleInUse()
	p.lock.Unlock()
}
func (p *Page) RLock(){ p.lock.RLock() }
func (p *Page) RUnlock(){ p.lock.RUnlock() }

func (p *Page) InUse() bool { return p.inUse }
func (p *Page) ToggleInUse() { p.inUse = !p.inUse }

func (p *Page) ToBytes() []byte {
	p.WriteToBuffer(p.fullBuffer)
	return p.fullBuffer
}

func (p *Page) WriteToBuffer(buff []byte) {
	buff[PAGETYPE_OFF] = byte(p.PageType)
	binary.LittleEndian.PutUint32(buff[PAGEID_OFF:], uint32(p.PageId))
	binary.LittleEndian.PutUint32(buff[NEXT_OFF:], uint32(p.Next))
	binary.LittleEndian.PutUint32(buff[PREV_OFF:], uint32(p.Prev))
	binary.LittleEndian.PutUint64(buff[LSN_OFF:], uint64(p.Lsn))
	copy(buff[PAGE_HEADER_LENGTH:], p.Body)
}

func (p *Page) ZeroOut() {
	p.TupleLen = 0
	p.isDirty = false
	p.PageId = 0
	p.Next = 0
	p.Prev = 0
}

func (p *Page) FromBytes(buff []byte) error { 
	if len(buff) < int(PAGE_HEADER_LENGTH) {
		return syscall.EINVAL // invalid page size
	}
	p.PageType = uint8(buff[PAGETYPE_OFF])
	p.Next = binary.LittleEndian.Uint64(buff[NEXT_OFF:])
	p.Prev = binary.LittleEndian.Uint64(buff[PREV_OFF:])
	p.Lsn = binary.LittleEndian.Uint64(buff[LSN_OFF:])
	p.Body = buff[PAGE_HEADER_LENGTH:]
	p.lock = &sync.RWMutex{}
	p.Cursor = 0
	p.fullBuffer = buff

	return nil
}

func LoadPage(fd int, id uint64, buff []byte) (*Page, error) {
	page := new(Page)
	// MMAP the entire node/page
	offset := int64(id) * int64(PAGE_SIZE)
	page.fd = fd
	page.PageId = id

	_, err := prims.Read(fd, buff, offset)
	if err != nil { return nil, err }

	page.FromBytes(buff)

	// set the done function to unmap the LoadPage
	return page, nil
}

func (p *Page) Flush(buff []byte) error { 
	offset := p.PageId * uint64(PAGE_SIZE)
	n, err := prims.Write(p.fd, buff, int64(offset))
	if n < int(PAGE_SIZE) {
		panic("didnt write it all")
	}
	if err != nil { return err }
	p.isDirty = false
	return nil
}

func (p *Page) GetVarRow(pid *PageId) []byte {
	// bin search the tuple list
	id := pid.InPID
	var tup []byte
	var low, high, mid int
	var val uint8
	high = int(p.TupleLen)
	for low <= high {
		mid = high / 2
		val = p.Body[(mid*TUPLE_SIZE)+idOffset]
		if id == val {
			tup = p.Body[(mid*TUPLE_SIZE):(mid*TUPLE_SIZE)+TUPLE_SIZE]
			break
		} else if id < val {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	offset := binary.LittleEndian.Uint16(tup[insetOffset:])
	length, _ := unpackFromBytes(tup[:2])
	return p.Body[offset:offset+length]
}

func (p *Page) GetFixRow(pid *PageId) []byte {
	bitmapsize := p.GetSlotCapacity() / 8
	slotSize := uint16(p.GetSlotSize())
	cursor := uint16(bitmapsize) + uint16(pid.InPID) * slotSize
	slice := p.Body[cursor:cursor+slotSize]
	return slice
}

// func InitMappings(fd int, id uint64, pageType PageType, zeroBuff []byte) {
// // 	for i := range 9 { zeroBuff[i] = 0 }
// // 	zeroBuff[0] = uint8(pageType)
// // 	binary.LittleEndian.PutUint64(zeroBuff[1:],id)
// // 	n, err := prims.Write(fd, zeroBuff, int64(id * uint64(PAGE_SIZE)))
// // 	if err != nil { panic(err.Error()) }
// // 	if n < int(PAGE_SIZE) {
// // 		panic("wrong length")
// // 	}
// }
//
func ReadNext(fd int, id uint64) uint64 {
	buff := make([]byte, 8)
	offset := int64(id * uint64(PAGE_SIZE) + NEXT_OFF)
	prims.Read(fd, buff, offset)
	return binary.LittleEndian.Uint64(buff)
}

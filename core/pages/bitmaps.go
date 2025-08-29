package pages

// import "fmt"

// GrabFreeSlot finds the first 0 bit in the bitmap, flips it to 1, and returns its global bit index.
func (p *Page)GrabFreeSlot() (id *PageId) {
	bitmap := p.Body[:(p.GetSlotCapacity()/8)]
	for byteIdx, b := range bitmap {
		if ^b != 0 { // if there is at least one 0 bit
			for bit := range 8 {
				if (b & (1 << bit)) == 0 {
					bitmap[byteIdx] |= (1 << bit) // set the bit

					pi := new(PageId)
					pi.PID = uint64(p.PageId)
					pi.InPID = uint8(byteIdx*8 + bit)
					return pi 
				}
			}
		}
	}
	return nil 
}

// ReleaseSlot clears the bit at the specified global bit index.
func (p *Page)ReleaseSlot(pid *PageId) {
	capacity := p.GetSlotCapacity()/8
	bitmap := p.Body[:capacity]

	byteIdx := pid.InPID / 8
	bit := pid.InPID % 8
	if byteIdx < capacity {
		bitmap[byteIdx] &^= (1 << bit) // clear the bit
	}
}

func (p *Page)GetSlotCapacity() uint8 {
	switch PageType(p.PageType) {
	case FILE_FIXED: return uint8((PAGE_SIZE-PAGE_HEADER_LENGTH)/72)
	default: return 0
	}
}
func (p *Page)GetSlotSize() uint8 {
	switch PageType(p.PageType) {
	case FILE_FIXED: return 72
	// case FID_IDX: 	return 41
	// case FTYPE_IDX: return 34
	// case FTIME_IDX: return 33
	default: return 0
	}

}

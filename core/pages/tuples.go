package pages

import (
	"encoding/binary"
	"errors"
)
const (
	TUPLE_SIZE = 5 // len(15)dirty(1) / inset(16) / id(8) = 5 bytes
	insetOffset = 2
	idOffset = 4
)

func (p *Page) GetVarSpace(pid *PageId) (error) {
	p.Lock()
	defer p.Unlock()

	var tup []byte
	var id uint8
	// bin search the tuple list
	var low, high, mid int
	high = (int(p.TupleLen) * TUPLE_SIZE) -1
	for low < high {
		mid = (low+high)/2

		id = p.Body[mid]
		if id == pid.InPID {
			tup = p.Body[mid:mid+TUPLE_SIZE]
			break
		} else if id < pid.InPID {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}  
	if tup == nil { return errors.New("no free space available") }

	tup[1] ^= 0x00 // flip last bit of length to indicate aliveness

	pid.Inset = binary.LittleEndian.Uint16(tup[insetOffset:])

	return nil
}

func (p *Page)PutFreeSpaceVar(pid *PageId, isRecursed ...bool) error {
	if len(isRecursed) == 0 || !isRecursed[0] { 
		p.Lock() 
		defer p.Unlock()
	}

	// loop through tuple list to find correct id
	// once its found mark as dirty..and go away
	var tup []byte
	var i uint16
	var id uint8

	for range int(p.TupleLen) {
		id = p.Body[i+TUPLE_SIZE]
		if id == pid.InPID  {
			tup = p.Body[i:i+TUPLE_SIZE]
			break
		}
		i += TUPLE_SIZE
	}
	if tup == nil { return errors.New("no free space available") }

	tup[1] ^= 0x01 // flip last bit of length to indicate deadness

	return nil
}

func packToBytes(value uint16, isDead bool) []byte {
    if value > 0x7FFF {
        panic("value exceeds 15 bits")
    }

    var packed uint16 = value
    if isDead {
        packed |= 0x8000 // set the highest bit
    }

    bytes := make([]byte, 2)
    binary.LittleEndian.PutUint16(bytes, packed)
    return bytes
}

func unpackFromBytes(data []byte) (value uint16, flag bool) {
    if len(data) < 2 {
        return 0, false
    }
    packed := binary.LittleEndian.Uint16(data)
    value = packed & 0x7FFF // mask lower 15 bits
    flag = (packed & 0x8000) != 0
    return value, flag
}


func (p *Page) GetLeftover(pid *PageId, delta uint16) (*PageId, error) {
	// create new tuple
	newFree := make([]byte, TUPLE_SIZE)
	copy(newFree, packToBytes(delta, true))
	binary.LittleEndian.PutUint16(newFree[insetOffset:], pid.Inset)

	// ADDS DELTA PACK TO PID BECAUSE THE ORIGINAL SPACE 
	// WHO OWNS THIS PID NEEDS THE RIGHT INSET STILL
	pid.Inset += delta 

	// incrememnt tuple coutn
	p.TupleLen++

	newList := make([]byte, p.TupleLen*TUPLE_SIZE)
	inserted := false

	var l, k int
	var curr, next, m uint8
	for ; m < p.TupleLen-1; m++ {
		curr = p.Body[l+idOffset]
		if l == 0 {
			if curr > 0 {
				newFree[idOffset] = curr - 1
				copy(newList[k:], newFree)
				k += TUPLE_SIZE
				inserted = true
			}
		}

		if l+TUPLE_SIZE+idOffset < len(p.Body) { 
			next = p.Body[l+TUPLE_SIZE+idOffset]
		} else { 
			next = curr + 2 
		}

		if !inserted && (next - curr) > 1 {
			newFree[idOffset] = curr + 1
			copy(newList[k:], newFree)
			k += TUPLE_SIZE
			inserted = true
		}

		copy(newList[k:], p.Body[l:l+TUPLE_SIZE])
		k += TUPLE_SIZE
		l += TUPLE_SIZE
	}
	copy(p.Body, newList)
	return nil, nil
}

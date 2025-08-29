package database

import (
	"encoding/binary"
	"errors"
	"log"
	"syscall"
	"mydb/core/pages"
	"mydb/core/prims"
	"mydb/core/types"
)

func (d *Database) NewFreePage(p types.PageLike) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	pageId := p.GetId()
	if d.DBHeader == nil { return errors.New("Header not initialized") }
	if pageId == 0 { return errors.New("Invalid page ID") }

	newPageOffset := int64(pageId) * int64(pages.PAGE_SIZE)

	// ZERO PAGE
	zeroBuff := make([]byte, pages.PAGE_SIZE)
	_, err := prims.Write(d.Fd, zeroBuff, newPageOffset)
	if err != nil { return nil }

	// UPDATE FREE LIST
	if d.Last != 0 {
		lastOffset := int64(d.Last * uint64(pages.PAGE_SIZE))
		buff := make([]byte, 8)
		binary.LittleEndian.PutUint64(buff, pageId)

		_, err = prims.Write(d.Fd, buff, lastOffset + pages.NEXT_OFF)
		if err != nil { return err }

	}
	d.Last = pageId

	return nil
}

func (d*Database) ClaimFreePage(pageType pages.PageType) (uint64, error) {
	if d.DBHeader == nil { return 0, errors.New("Header not initialized") }

	d.lock.Lock()
	defer d.lock.Unlock()

	var fPId uint64
	var err error
	// Store the free list pointer
	if d.Next == 0 {
		if d.Max == 0 || d.Total >= d.Max-1 {
			// No free pages available
			err = d.AllocateNewPages()
			if err != nil { return 0, err }
		}
		d.Total++

		fPId = d.Total
	}

	// Update the free list pointer to the next page
	d.Next = pages.ReadNext(d.Fd, fPId)

	return fPId, err
}

func (d *Database) AllocateNewPages() (error){
	if d.DBHeader == nil { return errors.New("No header in database") }
	// Allocate a new page
	d.Max += 100

	err := syscall.Ftruncate(d.Fd, int64(d.Max * uint64(pages.PAGE_SIZE)))
	if err != nil {
		log.Printf("Error truncating file to new page size: %v", err)
		return errors.New("Syscall error in allocate new page")
	}
	return nil
}



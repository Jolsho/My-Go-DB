package database

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"mydb/core/pages"
	"mydb/core/table"
	"mydb/core/types"
	"mydb/fileT"
)

type Database struct {
	*DBHeader
	FilePath 	string
	File 		*os.File
	Fd 			int

	fileTableIn chan *types.DbMessage
}
const (
	TOTAL_PAGES_OFF = 0
	NEXT_FREE_OFF   = 4
	LAST_FREE_OFF   = 8

	HEADER_SIZE     = 16 // Total size of the header in bytes
)

type DBHeader struct { 
	Total uint64
	Max uint64
	Next uint64
	Last uint64
	lock *sync.Mutex
	zeroBuff 	[]byte
}

func DBHeaderFromBytes(buff []byte) *DBHeader {
	dbh := &DBHeader{ 
		Total: binary.LittleEndian.Uint64(buff[:]),
		Next:binary.LittleEndian.Uint64(buff[NEXT_FREE_OFF:]) , 
		Last:binary.LittleEndian.Uint64(buff[LAST_FREE_OFF:]),
		lock: &sync.Mutex{},
		zeroBuff: make([]byte, pages.PAGE_SIZE),
	}
	return dbh
}

func (d *Database) GetFd() int { return d.Fd }
func (d *Database) Start(filePath string) error {
	d.FilePath = filePath
	if len(d.FilePath) < 1 { return errors.New("File path not set") }

	file, err := os.OpenFile(d.FilePath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil { 
		if os.IsNotExist(err) {
			// Create the file if it does not exist
			file, err = os.Create(d.FilePath)
			if err != nil {
				return errors.New("Failed to create database file: " + err.Error())
			}
		} else {
			return errors.New("Failed to open database file: " + err.Error())
		}
	}
	d.File = file
	d.Fd = int(file.Fd())

	defer func() {if err != nil { d.Close() }}()

	size, err := file.Seek(0, io.SeekEnd)
	if size < int64(pages.PAGE_SIZE) || err != nil { 
		if err != nil {
			return errors.New("Failed to seek to end of database file: " + err.Error())
		}
		// If we reach here, it means the file is empty or not initialized

		err = file.Truncate(int64(pages.PAGE_SIZE))
		if err != nil { return errors.New("Failed to truncate database file: " + err.Error()) }
	}
	buff := make([]byte, pages.PAGE_SIZE)
	metaPage, err := pages.LoadPage(d.Fd, 0, buff)
	if err != nil { return err }

	d.DBHeader = DBHeaderFromBytes(metaPage.Body[:HEADER_SIZE])
	metaPage.Cursor = HEADER_SIZE

	// FILES TABLE
	ft := &fileT.FileTable{BaseTable:new(table.BaseTable)}
	d.fileTableIn, err = table.StartTable(ft, d, metaPage)
	if err != nil { return err }

	return nil
}

func (d *Database) Close() error {
	// Seek to start
	_, err := d.File.Seek(0, 0)
	if err != nil { return err }

	// Write the header
	err = binary.Write(d.File, types.Direction, d.DBHeader)
	if err != nil { return err }

	if d.File != nil {
		d.File.Close()
		d.File = nil
		d.Fd = -1
	}
	return nil
}


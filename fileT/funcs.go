package fileT

import (
	"encoding/hex"
	"errors"
	"mydb/core/indexes"
	"mydb/core/pages"
	"mydb/core/types"
	"mydb/utils"
	"os"
)

const (
	BASE_PATH = "/var/lib/socialz/"
)

type GetFileMsg struct {
	Uid string
	Hash  [32]byte
}
type GetFileOut struct {
	Header *FileHeader
	Blobc  *chan []byte
	Error error
}

func (t *FileTable) GetFile(m *GetFileMsg, outPut chan any) {
	out := new(GetFileOut)

	// create the key
	uidBytes, err := hex.DecodeString(m.Uid)
	if err != nil { 
		out.Error = err
		outPut <- out
		return
	}
	m.Hash[16] = indexes.IS_CLEAN
	key := append(uidBytes, m.Hash[:17]...)

	// Get the offset for the file row
	q := NewFIDQuery(t.Index, key)
	rawPids, err := q.GetEntries(false, true)
	if err != nil { 
		out.Error = err 
		outPut <- out
		return
	}

	pid := pages.NewPageId(rawPids[0])
	// get the page for the blob
	pl, err := t.GetPage(pid)
	if err != nil { 
		out.Error = err
		outPut <- out
		return
	}
	p := pl.(*pages.Page)
	p.RLock()

	row := t.GetRow().(*FileRow)
	rowBytes := p.GetFixRow(pid)
	row.FromBytes(rowBytes)
	out.Header = row.ToFileHeader()

	p.RUnlock()

	blobc := make(chan []byte, 1)
	out.Blobc = &blobc
	go func() {
		defer close(blobc)
		// initialize variables
		file, err := os.Open(BASE_PATH + m.Uid + "/" + out.Header.Id)
		if err != nil { 
			if errors.Is(err, os.ErrNotExist) {
			}
			return 
		}
		chunks := make([][]byte, 4)
		for i := range len(chunks) {
			chunks[i] = make([]byte, utils.BUFFER_SIZE)
		}

		i := 0
		sent := 0
		var n int
		for sent < int(out.Header.Size) {
			n, err = file.Read(chunks[i])
			if err != nil { return }

			*out.Blobc <- chunks[i]
			sent += n
		}
	}()
	outPut <- out
}

type InsertFileMsg struct {
	Uid     string
	Size    int64
	FileType int8
	Time    int64
	Hash    [32]byte
}
type InsertFileOut struct { Error error }

func (t *FileTable)InsertFile(m *InsertFileMsg, outPut chan any) {
	out := new(InsertFileOut)
	var err error

	uidBytes, err := hex.DecodeString(m.Uid) 
	if err != nil { 
		out.Error = err
		outPut <- out
		return
	}
	// create the key
	m.Hash[16] = indexes.IS_CLEAN
	key := append(uidBytes, m.Hash[:17]...)

	row := t.GetRow().(*FileRow)
	row.Uid = [16]byte(uidBytes)
	row.Hash = [16]byte(m.Hash[:16])
	row.Size =m.Size
	row.FileType = m.FileType
	row.CreatedAt = m.Time
	row.RefCount = 1
	row.Blob = 0
	row.Padding = [3]byte{}

	//	check if the file already exists
	q := NewFIDQuery(t.Index, key)
	offsets, err := q.GetEntries(false, true)
	if !errors.Is(err, indexes.EntryNotFoundError) || len(offsets) > 0 {
		out.Error = err
		outPut <- out
		return
	}

	trxId, err := utils.RandomInt32()
	if err != nil { 
		out.Error = err
		outPut <- out
		return
	}

	// Claim space in table and set to row
	pid, err := t.FSM.GetFixedSpace(uint16(FILE_ROW_SIZE), pages.FILE_FIXED, trxId)
	if err != nil { 
		out.Error = err
		outPut <- out
		return
	}
	row.SetOffset(pid.Pack())

	page, err := t.GetPage(pid)
	if err != nil {
		out.Error = err
		outPut <- out
		return
	}
	p := page.(*pages.Page)
	p.Lock()
	defer p.Unlock()
	//
	// initialize actions and vals for logger/engine
	axs := make([]*types.Action, 1)
	vals := make([]*[]byte, 1)
	// Create the WAL action for the row
	axs[0] = types.InsertAx(types.NLBlob, pid.Pack(), FILE_ROW_SIZE)
	// Marshal the row
	rowBytes := row.ToBytes()
	vals[0] = &rowBytes
	t.PutRow(row)

	// ALl are pending
	p.Lsn, out.Error = t.Logger.NewTxn(&axs, &vals, trxId) 
	if out.Error != nil { 
		outPut <- out
		return
	}

	copy(p.GetFixRow(pid), rowBytes)

	out.Error = NewFileEntry(t.Index, rowBytes, trxId)
	if out.Error != nil { 
		outPut <- out
		return
	}
	t.Logger.CommitTxn(trxId)

	outPut <- out
}

func (t *FileTable)DeleteFile(m *GetFileMsg, outPut chan any) {
	out := new(InsertFileOut)

	// Search to make sure it doesnt exist
	// create the key
	uidBytes, err := hex.DecodeString(m.Uid)
	if err != nil { 
		out.Error = err
		outPut <- out
		return
	}
	m.Hash[16] = indexes.IS_CLEAN
	key := append(uidBytes, m.Hash[:17]...)

	q := NewFIDQuery(t.Index, key)
	rawPids, err := q.GetEntries(false,true)
	if err != nil { 
		out.Error = err
		outPut <- out
		return
	}

	pid := pages.NewPageId(rawPids[0])
	pl, err := t.GetPage(pid)
	if err != nil {
		out.Error = err
		outPut <- out
		return
	}
	p := pl.(*pages.Page)

	p.Lock()
	defer p.Unlock()
	p.PageId = pid.PID
	p.PageType = uint8(pages.FILE_FIXED)

	// read the row
	row := t.GetRow().(*FileRow)
	rowBytes := p.GetFixRow(pid)
	row.FromBytes(rowBytes)

	axs, vals := make([]*types.Action, 1), make([]*[]byte, 1)
	axs[0] = types.DeleteAx(types.NLBlob, pid.Pack(), FILE_ROW_SIZE)
	vals[0] = &rowBytes

	trxId, err := utils.RandomInt32()
	if err != nil { 
		out.Error = err
		outPut <- out
		return
	}

	p.Lsn, out.Error = t.Logger.NewTxn(&axs, &vals, trxId)
	if out.Error != nil {
		outPut <- out
		return
	}

	// just decriment count and return
	refCount := row.GetColumn("Ref_Count").(int32)
	if refCount > 1 {
		row.SetColumn("Ref_Count", refCount-1) 
		t.Logger.CommitTxn(trxId)
		outPut<-out
		return
	}

	// free up the row
	out.Error = t.GetFSM().PutFixedSpace(FILE_ROW_SIZE, pid, trxId)
	if out.Error != nil {
		t.Logger.CancelTxn(trxId)
		outPut <- out
		return
	}

	// Log deleting the index entries
	out.Error = DeleteFileEntry(t.Index, row.ToBytes(), trxId)
	if out.Error != nil {
		t.Logger.CancelTxn(trxId)
		outPut <- out
		return
	}

	t.Logger.CommitTxn(trxId)
	outPut<-out
}


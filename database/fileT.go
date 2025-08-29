package database

import (
	"errors"
	"mydb/core/types"
	"mydb/fileT"
)

func (d *Database) GetFile(uid string, hash [32]byte,
) (*fileT.FileHeader, *chan []byte, error) {
	msg := &types.DbMessage{
		Code: types.GET_FILE,
		Msg: &fileT.GetFileMsg{
			Uid: uid,
			Hash:  hash,
		},
		Output: make(chan any, 1),
	}
	d.fileTableIn <- msg
	result := <- msg.Output
	if res, ok := result.(*fileT.GetFileOut); ok {
		return res.Header, res.Blobc, res.Error
	}
	return nil, nil, errors.New("Invalid response from file table")
}

func (d *Database) InsertFile(
	uid string, size int64, fileType int8, 
	Hash [32]byte, Time int64,
) error {
	msg := &types.DbMessage{
		Code: types.INSERT_FILE,
		Msg: &fileT.InsertFileMsg{
			Uid:      uid,
			Hash:     Hash,
			Size:     size,
			FileType: fileType,
			Time:     Time,
		},
		Output: make(chan any, 1),
	}
	d.fileTableIn <- msg
	result := <- msg.Output
	if res, ok := result.(*fileT.InsertFileOut); ok {
		return res.Error
	}
	return errors.New("Invalid response from file table")
}

func (d *Database) DeleteFile(uid string, hash [32]byte) error {
	msg := &types.DbMessage{
		Code: types.DELETE_FILE,
		Msg: &fileT.GetFileMsg{
			Uid: 	uid,
			Hash:  hash,
		},
		Output: make(chan any, 1),
	}
	d.fileTableIn <- msg
	result := <- msg.Output
	if res, ok := result.(*fileT.InsertFileOut); ok {
		return res.Error
	}
	return errors.New("Invalid response from file table")
}


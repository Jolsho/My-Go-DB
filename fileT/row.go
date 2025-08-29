package fileT

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"
)

const ( FILE_ROW_SIZE = 72)

type FileRow struct {
	Uid			[16]byte
	Hash 		[16]byte
	Offset 		uint64
	Size 		int64
	FileType 	int8
	CreatedAt 	int64
	RefCount	int32
	Blob 		uint64
	Padding 	[3]byte
}
// SIZE = 16 + 8 + 8 + 1 + 8 + 16 + 4 + 8 + 3 = 72
// about 55 entries per page

func (r *FileRow) GetAllColumnNames() []string { 
	fieldList := []string{
		"Uid", "Hash", "Offset", "Size", 
		"File_Type", "Created_At",
		"Blob", "Ref_Count",
	}
	return fieldList 
}

func (r *FileRow) GetColumn(i string) any {
	switch i {
	case "Uid": 		return r.Uid
	case "Offset": 		return r.Offset
	case "Size": 		return r.Size
	case "File_Type": 	return r.FileType
	case "Created_At": 	return r.CreatedAt
	case "Hash": 		return r.Hash
	case "Ref_Count": 	return r.RefCount
	case "Blob": 		return r.Blob
	default: return nil
	}
}

func (r *FileRow) SetColumn(i string, v any) {
	switch i {
	case "Uid": 		r.Uid = v.([16]byte)
	case "Offset": 		r.Offset = v.(uint64)
	case "File_Type": 	r.FileType = v.(int8)
	case "Created_At": 	r.CreatedAt = v.(int64)
	case "Hash": 		r.Hash = v.([16]byte)
	case "Size": 		r.Size = v.(int64)
	case "Ref_Count": 	r.RefCount = v.(int32)
	case "Blob": 		r.Blob = v.(uint64)
	case "Padding":		r.Padding = v.([3]byte)
	case "": return
	}
}

func (r *FileRow) SetBlob(off uint64) { r.Blob = off }
func (r *FileRow) GetOffset() uint64 { return r.Offset }
func (r *FileRow) SetOffset(off uint64) { r.Offset = off }

func (r *FileRow) ToBytes() []byte {
	buff := make([]byte, FILE_ROW_SIZE)
	copy(buff, r.Uid[:])
	copy(buff[16:], r.Hash[:])
	binary.LittleEndian.PutUint64(buff[32:], r.Offset)
	binary.LittleEndian.PutUint64(buff[40:], uint64(r.Size))
	buff[48] = byte(r.FileType)
	binary.LittleEndian.PutUint64(buff[49:], uint64(r.CreatedAt))
	binary.LittleEndian.PutUint32(buff[57:], uint32(r.RefCount))
	binary.LittleEndian.PutUint64(buff[61:], r.Blob)
	copy(buff[69:], r.Padding[:])
	return buff
}

func (r *FileRow) FromBytes(buff []byte) {
	if len(buff) < FILE_ROW_SIZE { 
		fmt.Println("FileRow From bytes, buff not long enough")
		return 
	}
	copy(r.Uid[:], buff[:16])
	copy(r.Hash[:], buff[16:])
	r.Offset = binary.LittleEndian.Uint64(buff[32:])
	r.Size = int64(binary.LittleEndian.Uint64(buff[40:]))
	r.FileType = int8(buff[48])
	r.CreatedAt = int64(binary.LittleEndian.Uint64(buff[49:]))
	r.RefCount = int32(binary.LittleEndian.Uint32(buff[57:]))
	r.Blob = binary.LittleEndian.Uint64(buff[61:])
	r.Padding = [3]byte{}
}

type FileHeader struct {
	Id 			 string
	Size 		int64
	FileType 	string
	CreatedAt 	string	
}

func (r *FileRow) ToFileHeader() *FileHeader {
	fh := new(FileHeader)
	fh.Id = hex.EncodeToString(r.Hash[:])
	fh.Size = r.Size
	fh.FileType = deriveFileTypeFromInt(r.FileType)
	y,m,d := time.Unix(r.CreatedAt, 0).Date()
	h,mi,_ := time.Unix(r.CreatedAt, 0).Clock()
	fh.CreatedAt = time.Date(y,m,d,h,mi,0,0,time.UTC).Format("2006-01-02 15:04")
	return fh
}

const (
	// File types
	Jpeg int8 = iota
	Png
	Gif
	Mp4
	Mpeg
	Json
	Pdf
	Zip
	Tar
	Gz
	Msword
	Vnd_ms_excel
	Misc
)

func deriveFileTypeFromInt(i int8) string {
	switch i {
	case Jpeg: 	return "image/jpeg"
	case Png: 	return "image/png"
	case Gif: 	return "image/gif"
	case Mp4: 	return "video/mp4"
	case Mpeg: 	return "audio/mpeg"
	case Json: 	return "application/json"
	case Pdf: 	return "application/pdf"
	case Zip: 	return "application/zip"
	case Tar: 	return "application/tar"
	case Gz: 	return "application/gz"
	case Msword:return "application/msword"
	case Vnd_ms_excel: return "application/vnd.ms-excel"
	default: 	return "Unknown"
	}
}

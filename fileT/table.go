package fileT

import (
	"sync"
	"mydb/core/pages"
	"mydb/core/table"
	"mydb/core/types"
)

type FileTable struct { *table.BaseTable }

func (t *FileTable) SetIndexAndColumns(metaPage *pages.Page) {
	t.Code = types.FILE_TABLE
	t.Index = NewFileIndex(t, metaPage)
	t.RowPool = &sync.Pool{
		New: func() any {return new(FileRow)},
	}
	t.Columns = map[string]table.Column{
		// Identifiers and metadata
		"Uid":   		{Name:"Uid", 		Type:types.UUID, 	},
		"Hash": 		{Name:"Hash", 		Type:types.UUID, 	},
		"Offset": 		{Name:"Offset", 	Type:types.Int64, 	},
		"Size":			{Name:"Size", 		Type:types.Int64, 	},
		"File_Type":	{Name:"File_Type", 	Type:types.Int8, 	},
		"Created_At": 	{Name:"Created_At", Type:types.Time, 	},
		"Ref_Count": 	{Name:"Ref_Count", 	Type:types.Int32, 	},
		// Heap data offset
		"Blob":   		{Name:"Blob", 		Type:types.Int64, 	},
		// Padding
		"Padding": 		{Name:"Padding", 	Type:types.FileRowPadding, },
	}
}

func (t *FileTable) Run(in chan *types.DbMessage) {
	MAIN: for msg := range in {
		switch msg.Code {
		case types.GET_FILE: t.GetFile(msg.Msg.(*GetFileMsg), msg.Output)
		case types.INSERT_FILE: t.InsertFile(msg.Msg.(*InsertFileMsg), msg.Output)
		case types.DELETE_FILE: t.DeleteFile(msg.Msg.(*GetFileMsg), msg.Output)
		// case types.RECOVERY: execution.ExecuteAction(t.GetLogger(), msg.Msg.(*types.Action))
		case types.TERMINATE: break MAIN
		}
	}
}

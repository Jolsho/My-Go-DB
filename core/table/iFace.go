package table

import (
	"mydb/core/cache"
	"mydb/core/fsm"
	"mydb/core/logger"
	"mydb/core/pages"
	"mydb/core/types"
)

func StartTable[T Table](t T, d types.DatabaseI, metaPage *pages.Page) (chan *types.DbMessage, error){
	t.SetFd(d.GetFd())
	t.SetDatabase(d)
	t.SetCache(cache.NewCache(t.GetFd()))

	logger, err := logger.StartLogger(d, metaPage, 
		func (id uint64) (types.PageLike, error) {return t.GetPage(pages.NewPageId(id))},
	)
	if err != nil { return nil, err }
	t.SetLogger(logger)

	t.SetFSM(fsm.NewFSM(d, logger, t.GetCache(), metaPage))
	metaPage.Cursor += 4 // only has one root

	t.SetIndexAndColumns(metaPage)

	chanl := make(chan *types.DbMessage)
	go t.Run(chanl)
	return chanl, nil
}


type Table interface {
	GetFd() int
	SetFd(int)
	GetCode() types.TableCode

	GetPage(*pages.PageId) (types.PageLike, error)

	GetDatabase() types.DatabaseI
	SetDatabase(types.DatabaseI)

	GetCache() *cache.Cache
	SetCache(*cache.Cache)

	GetFSM() *fsm.FSM
	SetFSM(*fsm.FSM)

	GetLogger() *logger.Logger
	SetLogger(*logger.Logger)

	GetColumns() map[string]Column
	SetColumns(map[string]Column)

	SetIndexAndColumns(*pages.Page)
	Run(chan *types.DbMessage)
}

type TableRow interface {
	GetColumn(string) any
	SetColumn(string, any)
	GetAllColumnNames() []string
	GetOffset() int64
	SetOffset(int64)
	ToBytes() []byte
	FromBytes([]byte)
}

type Column struct {
	Name 	string
	Type 	types.DataType
}

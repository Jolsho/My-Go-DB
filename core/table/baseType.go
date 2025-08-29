package table

import (
	"sync"
	"mydb/core/cache"
	"mydb/core/fsm"
	"mydb/core/indexes"
	"mydb/core/logger"
	"mydb/core/pages"
	"mydb/core/types"
)

type BaseTable struct {
	Code 	  	types.TableCode
	Fd 			int
	db 			types.DatabaseI
	Logger 		*logger.Logger
	FSM 		*fsm.FSM
	Cache 		*cache.Cache
	Index 		*indexes.Idx
	Columns 	map[string]Column
	RowPool		*sync.Pool
}

func (t *BaseTable) GetCode() types.TableCode { return t.Code }
func (t *BaseTable) GetFd() int { return t.Fd }
func (t *BaseTable) SetFd(fd int) { t.Fd = fd }
func (t *BaseTable) GetColumns() map[string]Column { return t.Columns }
func (t *BaseTable) SetColumns(columns map[string]Column) { t.Columns = columns }
func (t *BaseTable) GetDatabase() types.DatabaseI { return t.db }
func (t *BaseTable) SetDatabase(db types.DatabaseI) { t.db = db }

func (t *BaseTable) GetCache() *cache.Cache { return t.Cache }
func (t *BaseTable) SetCache(cache *cache.Cache) { t.Cache = cache }

func (t *BaseTable) GetFSM() *fsm.FSM { return t.FSM }
func (t *BaseTable) SetFSM(fsm *fsm.FSM) { t.FSM = fsm }

func (t *BaseTable) GetLogger() *logger.Logger { return t.Logger }
func (t *BaseTable) SetLogger(logger *logger.Logger) { t.Logger = logger }

func (t *BaseTable) GetRow() any { return t.RowPool.Get() }
func (t *BaseTable) PutRow(row any) { t.RowPool.Put(row) }

func (t *BaseTable) GetPage(pid *pages.PageId) (types.PageLike, error) { 
	p, ok := t.Cache.Get(pid.PID)
	if !ok {
		var err error
		buff := t.Cache.GetBuffer()
		p, err = pages.LoadPage(t.db.GetFd(), pid.PID,	buff)
		if err != nil { return nil, err }

		t.Cache.Set(p)
	}
	return p, nil
}



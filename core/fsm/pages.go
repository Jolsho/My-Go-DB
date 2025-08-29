package fsm

import (
	"mydb/core/pages"
	"mydb/core/types"
)

func (f *FSM)GetPage(pid *pages.PageId) (*pages.Page, error) {
	var err error
	p, ok := f.Cache.Get(pid.PID)
	var page *pages.Page
	if !ok {
		buff := f.Cache.GetBuffer()
		page, err = pages.LoadPage(f.Db.GetFd(), pid.PID, buff)
		if err != nil { return nil, err }

		f.Cache.Set(page)

	} else { 
		page = p.(*pages.Page) 
	}
	return page, nil
}

func (f *FSM)ClaimFreePage(pType pages.PageType, trxId int32) (*pages.Page, error) {
	// create a new node for the leaf
	newId, err := f.Db.ClaimFreePage(pType)
	if err != nil { return nil, err }

	axs, vs := make([]*types.Action, 1), make([]*[]byte, 1)
	axs[0], vs[0] = types.AtomicAx(types.NEWPAGE, types.Page, newId), &[]byte{}

	page, err := f.GetPage(pages.NewPageId(newId))
	if err != nil { return nil, err }
	page.PageId = newId
	page.PageType = uint8(pType)

	return page, nil
}

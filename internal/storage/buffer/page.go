package buffer

import (
	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type Page struct {
	PageId  page.Id
	Page    *page.Page
	isDirty bool
}

func newPage(pageId page.Id) (*Page, error) {
	p, err := page.NewPage(directio.AlignedBlock(page.PageSize))
	if err != nil {
		return nil, err
	}
	return &Page{
		PageId:  pageId,
		Page:    p,
		isDirty: false,
	}, nil
}

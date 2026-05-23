package buffer

import (
	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type BufferPage struct {
	PageId  page.Id
	Page    *page.Page
	isDirty bool
}

func newBufferPage(pageId page.Id) (*BufferPage, error) {
	p, err := page.NewPage(directio.AlignedBlock(page.PageSize))
	if err != nil {
		return nil, err
	}
	return &BufferPage{
		PageId:  pageId,
		Page:    p,
		isDirty: false,
	}, nil
}

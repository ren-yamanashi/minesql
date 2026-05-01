package buffer

import (
	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type BufferPage struct {
	PageId  page.PageId
	Page    *page.Page
	IsDirty bool
}

func NewBufferPage(pageId page.PageId) (*BufferPage, error) {
	p, err := page.NewPage(directio.AlignedBlock(page.PageSize))
	if err != nil {
		return nil, err
	}
	return &BufferPage{
		PageId:  pageId,
		Page:    p,
		IsDirty: false,
	}, nil
}

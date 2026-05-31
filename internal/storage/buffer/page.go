package buffer

import (
	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type Page struct {
	pageId      page.Id
	data        *page.Page
	isDirty     bool
	pinCount    int
	latch       *RWLatch
	modifyCount uint64 // X ラッチ取得時に増加する更新カウンタ (順次走査の位置復元判定用)
}

func (p *Page) PageId() page.Id  { return p.pageId }
func (p *Page) Data() *page.Page { return p.data }

func NewPage(pageId page.Id) (*Page, error) {
	p, err := page.NewPage(directio.AlignedBlock(page.Size))
	if err != nil {
		return nil, err
	}
	return &Page{
		pageId:   pageId,
		data:     p,
		isDirty:  false,
		pinCount: 0,
		latch:    NewRWLatch(),
	}, nil
}

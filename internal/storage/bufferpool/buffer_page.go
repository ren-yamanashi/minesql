package bufferpool

import (
	"minesql/internal/storage/page"
)

type BufferPage struct {
	PageId page.PageId
	Page   *page.Page
	// 最近アクセスされたかどうか
	Referenced bool
	// ページが変更されたかどうか
	IsDirty bool
}

func NewBufferPage(pageId page.PageId) *BufferPage {
	return &BufferPage{
		PageId:     pageId,
		Page:       &page.Page{},
		Referenced: false,
		IsDirty:    false,
	}
}

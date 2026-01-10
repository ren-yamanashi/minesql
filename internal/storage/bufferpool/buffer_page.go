package bufferpool

import (
	"minesql/internal/storage/disk"
)

type BufferPage struct {
	PageId disk.PageId
	Page   *disk.Page
	// 最近アクセスされたかどうか
	Referenced bool
	// ページが変更されたかどうか
	IsDirty bool
}

func newBufferPage(pageId disk.PageId) *BufferPage {
	return &BufferPage{
		PageId:  pageId,
		Page:    &disk.Page{},
		IsDirty: false,
	}
}

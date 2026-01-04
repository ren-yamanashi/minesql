package bufferPool

import (
	"minesql/internal/storage/disk"
)

type Page [disk.PAGE_SIZE]uint8

type BufferPage struct {
	PageId  disk.PageId
	Page    *Page
	// 最近アクセスされたかどうか
	Referenced bool
	// ページが変更されたかどうか
	IsDirty bool
}

func NewBufferPage(pageId disk.PageId) *BufferPage {
	return &BufferPage{
		PageId:  pageId,
		Page:    &Page{},
		IsDirty: false,
	}
}

package bufferpool

import (
	"minesql/internal/storage/disk"
)

type BufferPage struct {
	OldPageId disk.OldPageId
	Page   *disk.Page
	// 最近アクセスされたかどうか
	Referenced bool
	// ページが変更されたかどうか
	IsDirty bool
}

func NewBufferPage(pageId disk.OldPageId) *BufferPage {
	return &BufferPage{
		OldPageId:     pageId,
		Page:       &disk.Page{},
		Referenced: false,
		IsDirty:    false,
	}
}

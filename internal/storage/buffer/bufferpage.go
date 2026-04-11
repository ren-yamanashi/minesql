package buffer

import (
	"minesql/internal/storage/page"

	"github.com/ncw/directio"
)

// BufferPage はバッファプール内のページ (バッファページ) を表す
type BufferPage struct {
	PageId  page.PageId // ページ ID
	Page    []byte      // ページデータ
	IsDirty bool        // ページが変更されたかどうか
}

// NewBufferPage は指定されたページ ID を持つ新しい BufferPage を作成する
func NewBufferPage(pageId page.PageId) *BufferPage {
	return &BufferPage{
		PageId:  pageId,
		Page:    directio.AlignedBlock(page.PAGE_SIZE),
		IsDirty: false,
	}
}

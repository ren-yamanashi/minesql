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

// GetWriteData は書き込み用のデータを取得する (IsDirty を true にセットする)
func (bp *BufferPage) GetWriteData() []byte {
	bp.IsDirty = true
	return bp.Page
}

// GetReadData は読み込み用のデータを取得する
func (bp *BufferPage) GetReadData() []byte {
	return bp.Page
}

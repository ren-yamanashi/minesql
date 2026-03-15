package bufferpool

import (
	"minesql/internal/storage/page"

	"github.com/ncw/directio"
)

// BufferPage はバッファプール内のページ (バッファページ) を表す
type BufferPage struct {
	// ページ ID
	PageId page.PageId
	// ページデータ
	Page []byte
	// ページが変更されたかどうか
	IsDirty bool
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

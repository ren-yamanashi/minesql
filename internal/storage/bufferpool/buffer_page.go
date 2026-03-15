package bufferpool

import (
	"minesql/internal/storage/page"

	"github.com/ncw/directio"
)

// BufferPage は、バッファプール内のページ (バッファページ) を表す構造体
type BufferPage struct {
	// ページ ID
	PageId page.PageId
	// ページデータ
	Page []byte
	// 最近アクセスされたかどうか
	Referenced bool
	// ページが変更されたかどうか
	IsDirty bool
}

// NewBufferPage は指定されたページ ID を持つ新しい BufferPage を作成する
func NewBufferPage(pageId page.PageId) *BufferPage {
	return &BufferPage{
		PageId:     pageId,
		Page:       directio.AlignedBlock(page.PAGE_SIZE),
		Referenced: false,
		IsDirty:    false,
	}
}

// GetWriteData は書き込み用のデータを取得する (IsDirty と Referenced を true にセットする)
func (bp *BufferPage) GetWriteData() []byte {
	bp.IsDirty = true
	bp.Referenced = true
	return bp.Page
}

// GetReadData は読み込み用のデータを取得する (Referenced のみを true にセットする)
func (bp *BufferPage) GetReadData() []byte {
	bp.Referenced = true
	return bp.Page
}

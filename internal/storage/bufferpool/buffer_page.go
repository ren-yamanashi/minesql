package bufferpool

import (
	"minesql/internal/storage/page"

	"github.com/ncw/directio"
)

type BufferPage struct {
	PageId page.PageId
	Page   []byte
	// 最近アクセスされたかどうか
	Referenced bool
	// ページが変更されたかどうか
	IsDirty bool
}

func NewBufferPage(pageId page.PageId) *BufferPage {
	return &BufferPage{
		PageId:     pageId,
		Page:       directio.AlignedBlock(directio.BlockSize),
		Referenced: false,
		IsDirty:    false,
	}
}

// 書き込み用のデータを取得する (IsDirty と Referenced を true にセットする)
func (bp *BufferPage) GetWriteData() []byte {
	bp.IsDirty = true
	bp.Referenced = true
	return bp.Page
}

// 読み込み用のデータを取得する (Referenced のみを true にセットする)
func (bp *BufferPage) GetReadData() []byte {
	bp.Referenced = true
	return bp.Page
}

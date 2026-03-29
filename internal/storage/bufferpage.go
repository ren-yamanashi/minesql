package storage

import (
	"github.com/ncw/directio"
)

// BufferPage はバッファプール内のページ (バッファページ) を表す
type BufferPage struct {
	// ページ ID
	PageId PageId
	// ページデータ
	Page []byte
	// ページが変更されたかどうか
	IsDirty bool
}

// NewBufferPage は指定されたページ ID を持つ新しい BufferPage を作成する
func NewBufferPage(pageId PageId) *BufferPage {
	return &BufferPage{
		PageId:  pageId,
		Page:    directio.AlignedBlock(PAGE_SIZE),
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

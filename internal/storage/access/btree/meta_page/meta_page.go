package metapage

import (
	"minesql/internal/storage/disk"
)

type MetaPage struct {
	// メタページ全体のバイト列
	data []byte
}

func NewMetaPage(data []byte) *MetaPage {
	return &MetaPage{data: data}
}

// メタページのヘッダーからルートページ ID を読み取る
func (mp *MetaPage) RootPageId() disk.PageId {
	return disk.ReadPageIdFrom(mp.data, 0)
}

// メタページのヘッダーにルートページ ID を設定する
func (mp *MetaPage) SetRootPageId(rootPageId disk.PageId) {
	rootPageId.WriteTo(mp.data, 0)
}

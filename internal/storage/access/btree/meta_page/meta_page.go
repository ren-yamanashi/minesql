package metapage

import (
	"minesql/internal/storage/page"
)

type MetaPage struct {
	data []byte // メタページ全体のバイト列 (先頭 8 byte はヘッダーで、ヘッダーにはルートページ ID が格納される)
}

// NewMetaPage は指定されたバイト列から MetaPage を生成する
func NewMetaPage(data []byte) *MetaPage {
	return &MetaPage{data: data}
}

// RootPageId はメタページのヘッダーからルートページ ID を読み取る
func (mp *MetaPage) RootPageId() page.PageId {
	return page.ReadPageIdFromPageData(mp.data, 0)
}

// SetRootPageId はメタページのヘッダーにルートページ ID を設定する
func (mp *MetaPage) SetRootPageId(rootPageId page.PageId) {
	rootPageId.WriteTo(mp.data, 0)
}

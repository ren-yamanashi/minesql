package btree

import (
	"minesql/internal/storage/page"
)

type metaPage struct {
	data []byte // メタページ全体のバイト列 (先頭 8 byte はヘッダーで、ヘッダーにはルートページ ID が格納される)
}

// newMetaPage は指定されたバイト列から metaPage を生成する
func newMetaPage(data []byte) *metaPage {
	return &metaPage{data: data}
}

// rootPageId はメタページのヘッダーからルートページ ID を読み取る
func (mp *metaPage) rootPageId() page.PageId {
	return page.ReadPageIdFromPageData(mp.data, 0)
}

// setRootPageId はメタページのヘッダーにルートページ ID を設定する
func (mp *metaPage) setRootPageId(rootPageId page.PageId) {
	rootPageId.WriteTo(mp.data, 0)
}

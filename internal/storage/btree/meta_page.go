package btree

import (
	"encoding/binary"
	"minesql/internal/storage/page"
)

// メタページのレイアウト:
//
//	offset 0-7:   ルートページ ID (8 バイト)
//	offset 8-15:  リーフページ数 (8 バイト)
//	offset 16-23: B+Tree の高さ (8 バイト)
type metaPage struct {
	data []byte
}

// newMetaPage は指定されたバイト列から metaPage を生成する
func newMetaPage(data []byte) *metaPage {
	return &metaPage{data: data}
}

// rootPageId はルートページ ID を読み取る
func (mp *metaPage) rootPageId() page.PageId {
	return page.ReadPageIdFromPageData(mp.data, 0)
}

// setRootPageId はルートページ ID を設定する
func (mp *metaPage) setRootPageId(rootPageId page.PageId) {
	rootPageId.WriteTo(mp.data, 0)
}

// leafPageCount はリーフページ数を読み取る
func (mp *metaPage) leafPageCount() uint64 {
	return binary.BigEndian.Uint64(mp.data[8:16])
}

// setLeafPageCount はリーフページ数を設定する
func (mp *metaPage) setLeafPageCount(count uint64) {
	binary.BigEndian.PutUint64(mp.data[8:16], count)
}

// height は B+Tree の高さ (ルートからリーフまでのレベル数) を読み取る
func (mp *metaPage) height() uint64 {
	return binary.BigEndian.Uint64(mp.data[16:24])
}

// setHeight は B+Tree の高さを設定する
func (mp *metaPage) setHeight(h uint64) {
	binary.BigEndian.PutUint64(mp.data[16:24], h)
}

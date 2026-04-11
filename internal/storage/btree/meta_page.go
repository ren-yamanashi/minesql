package btree

import (
	"encoding/binary"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/page"
)

// メタページのレイアウト (Body 内):
//   - offset 0-7:   SlottedPage ヘッダー (予約領域等)
//   - offset 8-15:  ルートページ ID (8 バイト)
//   - offset 16-23: リーフページ数 (8 バイト)
//   - offset 24-31: B+Tree の高さ (8 バイト)
type metaPage struct {
	pg *page.Page
}

// createMetaPage は新しいメタページを作成して初期化する
func createMetaPage(pg *page.Page) *metaPage {
	node.NewSlottedPage(pg.Body).Initialize()
	return &metaPage{pg: pg}
}

// newMetaPage は既存のメタページを開く
func newMetaPage(pg *page.Page) *metaPage {
	return &metaPage{pg: pg}
}

// rootPageId はルートページ ID を読み取る
func (mp *metaPage) rootPageId() page.PageId {
	return page.ReadPageIdFromPageData(mp.pg.Body, 8)
}

// setRootPageId はルートページ ID を設定する
func (mp *metaPage) setRootPageId(rootPageId page.PageId) {
	rootPageId.WriteTo(mp.pg.Body, 8)
}

// leafPageCount はリーフページ数を読み取る
func (mp *metaPage) leafPageCount() uint64 {
	return binary.BigEndian.Uint64(mp.pg.Body[16:24])
}

// setLeafPageCount はリーフページ数を設定する
func (mp *metaPage) setLeafPageCount(count uint64) {
	binary.BigEndian.PutUint64(mp.pg.Body[16:24], count)
}

// height は B+Tree の高さ (ルートからリーフまでのレベル数) を読み取る
func (mp *metaPage) height() uint64 {
	return binary.BigEndian.Uint64(mp.pg.Body[24:32])
}

// setHeight は B+Tree の高さを設定する
func (mp *metaPage) setHeight(h uint64) {
	binary.BigEndian.PutUint64(mp.pg.Body[24:32], h)
}

package btree

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// メタページ内のオフセット
const (
	metaRootPageIdOffset    = 0
	metaLeafPageCountOffset = 8
	metaHeightOffset        = 16
)

// メタページ
//   - offset 0-7:   ルートページ ID (8 バイト)
//   - offset 8-15:  リーフページ数 (8 バイト)
//   - offset 16-23: B+Tree の高さ (8 バイト)
type metaPage struct {
	data    *page.Page
	bufPage *buffer.Page // 書き込み API で MarkModified を自動呼び出しするためのバックポインタ
}

// newMetaPage は既存のメタページを開く
func newMetaPage(bufPage *buffer.Page) *metaPage {
	return &metaPage{data: bufPage.Data(), bufPage: bufPage}
}

// rootPageId はルートページ ID を読み取る
func (mp *metaPage) rootPageId() page.Id {
	return page.ReadId(mp.data.Body(), metaRootPageIdOffset)
}

// leafPageCount はリーフページ数を読み取る
func (mp *metaPage) leafPageCount() uint64 {
	return binary.BigEndian.Uint64(mp.data.Body()[metaLeafPageCountOffset : metaLeafPageCountOffset+8])
}

// height は B+Tree の高さを読み取る
func (mp *metaPage) height() uint64 {
	return binary.BigEndian.Uint64(mp.data.Body()[metaHeightOffset : metaHeightOffset+8])
}

// setRootPageId はルートページ ID を設定する
func (mp *metaPage) setRootPageId(rootPageId page.Id) {
	mp.bufPage.WriteBodyAt(metaRootPageIdOffset, rootPageId.Bytes())
}

// setLeafPageCount はリーフページ数を設定する
func (mp *metaPage) setLeafPageCount(count uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], count)
	mp.bufPage.WriteBodyAt(metaLeafPageCountOffset, buf[:])
}

// setHeight は B+Tree の高さを設定する
func (mp *metaPage) setHeight(h uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], h)
	mp.bufPage.WriteBodyAt(metaHeightOffset, buf[:])
}

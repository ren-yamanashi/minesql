package access

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// UNDO ページのレイアウト (Body 内):
//
// ヘッダー (4 バイト):
//   - offset 0-1: usedBytes (uint16) - ボディの使用済みバイト数
//   - offset 2-3: nextPageNumber (uint16) - 次の UNDO ページの PageNumber (0 = なし)
//
// ボディ:
//   - offset 4+:  UNDO レコードが先頭から順に詰められる
//
// Page LSN は Page 構造体のヘッダー (raw data の先頭 4 バイト) で管理される

const undoPageHeaderSize = 4 // usedBytes (2B) + nextPageNumber (2B)

type UndoPage struct {
	pg     *page.Page
	header []byte // pg.Body[:4] - usedBytes + nextPageNumber
	body   []byte // pg.Body[4:] - UNDO レコード
}

// NewUndoPage は Page から UndoPage を作成する (Page を UndoPage として扱う)
func NewUndoPage(pg *page.Page) *UndoPage {
	body := pg.Body
	return &UndoPage{
		pg:     pg,
		header: body[:undoPageHeaderSize],
		body:   body[undoPageHeaderSize:],
	}
}

// Initialize は UNDO ページを初期化する
func (p *UndoPage) Initialize() {
	binary.BigEndian.PutUint16(p.header[0:2], 0) // usedBytes
	binary.BigEndian.PutUint16(p.header[2:4], 0) // nextPageNumber
}

// Append は UNDO レコードをボディに追加する
//
// 空き不足の場合は false を返す
func (p *UndoPage) Append(record []byte) bool {
	used := int(p.UsedBytes())
	if used+len(record) > len(p.body) {
		return false
	}
	copy(p.body[used:], record)
	binary.BigEndian.PutUint16(p.header[0:2], uint16(used+len(record)))
	return true
}

// RecordAt はボディ内の指定 offset のレコードを読み取る
func (p *UndoPage) RecordAt(offset int) []byte {
	if offset >= len(p.body) {
		return nil
	}
	if offset+undoRecordHeaderSize > len(p.body) {
		return nil
	}
	// DataLen は UNDO レコードヘッダーの末尾 2 バイトに格納されている
	dataLenOffset := offset + undoRecordHeaderSize - 2
	dataLen := int(binary.BigEndian.Uint16(p.body[dataLenOffset : dataLenOffset+2]))
	totalLen := undoRecordHeaderSize + dataLen
	if offset+totalLen > len(p.body) {
		return nil
	}
	// レコード全体を返す
	return p.body[offset : offset+totalLen]
}

// UsedBytes はボディの使用済みバイト数を返す
func (p *UndoPage) UsedBytes() uint16 {
	return binary.BigEndian.Uint16(p.header[0:2])
}

// NextPageNumber は次の UNDO ページの PageNumber を返す (0 = なし)
func (p *UndoPage) NextPageNumber() uint16 {
	return binary.BigEndian.Uint16(p.header[2:4])
}

// SetNextPageNumber は次の UNDO ページの PageNumber を設定する
func (p *UndoPage) SetNextPageNumber(pn uint16) {
	binary.BigEndian.PutUint16(p.header[2:4], pn)
}

// FreeSpace はボディ内の空き容量を返す
func (p *UndoPage) FreeSpace() int {
	return len(p.body) - int(p.UsedBytes())
}

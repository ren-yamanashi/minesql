package access

import "encoding/binary"

// UNDO ページのレイアウト:
//
// ヘッダー (12 バイト):
//   - offset 0-7:   SlottedPage ヘッダー (pageLSN 等)
//   - offset 8-9:   usedBytes (uint16) - ボディの使用済みバイト数
//   - offset 10-11: nextPageNumber (uint16) - 次の UNDO ページの PageNumber (0 = なし)
//
// ボディ:
//   - offset 12+:   UNDO レコードが先頭から順に詰められる

const undoPageHeaderSize = 12 // SlottedPage ヘッダー (8B) + usedBytes (2B) + nextPageNumber (2B)

type UndoPage struct {
	header []byte // ページデータの先頭 12 バイト
	body   []byte // ページデータの 12 バイト目以降
}

// NewUndoPage は与えられたページデータから UndoPage を作成する
func NewUndoPage(data []byte) *UndoPage {
	return &UndoPage{
		header: data[:undoPageHeaderSize],
		body:   data[undoPageHeaderSize:],
	}
}

// Initialize は UNDO ページを初期化する
func (p *UndoPage) Initialize() {
	// SlottedPage ヘッダーを初期化 (pageLSN = 0)
	binary.BigEndian.PutUint16(p.header[0:2], 0) // numSlots (未使用)
	binary.BigEndian.PutUint16(p.header[2:4], 0) // freeOffset (未使用)
	binary.BigEndian.PutUint32(p.header[4:8], 0) // pageLSN
	// UNDO ページ固有ヘッダー
	binary.BigEndian.PutUint16(p.header[8:10], 0)  // usedBytes
	binary.BigEndian.PutUint16(p.header[10:12], 0) // nextPageNumber
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
	binary.BigEndian.PutUint16(p.header[8:10], uint16(used+len(record)))
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
	// DataLen はレコードヘッダーの末尾 2 バイトに格納されている
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
	return binary.BigEndian.Uint16(p.header[8:10])
}

// NextPageNumber は次の UNDO ページの PageNumber を返す (0 = なし)
func (p *UndoPage) NextPageNumber() uint16 {
	return binary.BigEndian.Uint16(p.header[10:12])
}

// SetNextPageNumber は次の UNDO ページの PageNumber を設定する
func (p *UndoPage) SetNextPageNumber(pn uint16) {
	binary.BigEndian.PutUint16(p.header[10:12], pn)
}

// FreeSpace はボディ内の空き容量を返す
func (p *UndoPage) FreeSpace() int {
	return len(p.body) - int(p.UsedBytes())
}

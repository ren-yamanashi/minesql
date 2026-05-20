package undo

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const (
	headerUsedBytesOffset      = 0
	headerNextPageNumberOffset = 2
	pageHeaderSize             = 6 // UsedBytes(2) + NextPageNum(2)
)

type Page struct {
	page   *page.Page
	header []byte
	body   []byte
}

func NewPage(page page.Page) *Page {
	body := page.Body
	return &Page{
		page:   &page,
		header: body[:pageHeaderSize],
		body:   body[pageHeaderSize:],
	}
}

// Initialize は Undo ページを初期化する
func (p *Page) Initialize() {
	binary.BigEndian.PutUint16(p.header[headerUsedBytesOffset:headerNextPageNumberOffset], 0) // usedBytes
	binary.BigEndian.PutUint16(p.header[headerNextPageNumberOffset:pageHeaderSize], 0)        // nextPageNumber
}

// Append は Undo レコードをボディに追加する
//
// 空き不足の場合は false を返す
func (p *Page) Append(record []byte) bool {
	used := int(p.UsedBytes())
	if used+len(record) > len(p.body) {
		return false
	}
	copy(p.body[used:], record)
	binary.BigEndian.PutUint16(p.header[headerUsedBytesOffset:headerNextPageNumberOffset], uint16(used+len(record)))
	return true
}

// RecordAt はボティ内の指定 offset のレコードを読み取る
func (p *Page) RecordAt(offset int) []byte {
	if offset >= len(p.body) {
		return nil
	}
	if offset+recordHeaderSize > len(p.body) {
		return nil
	}
	dataLen := int(binary.BigEndian.Uint16(p.body[offset+headerDataLenOffset : offset+recordHeaderSize]))
	totalLen := recordHeaderSize + dataLen
	if offset+totalLen > len(p.body) {
		return nil
	}
	return p.body[offset : offset+totalLen]
}

// UsedBytes はボディの使用済みバイト数を返す
func (p *Page) UsedBytes() uint16 {
	return binary.BigEndian.Uint16(p.header[headerUsedBytesOffset:headerNextPageNumberOffset])
}

// NextPageNumber は次の Undo ページの PageNumber を返す
func (p *Page) NextPageNumber() page.PageNumber {
	return page.PageNumber(binary.BigEndian.Uint32(p.header[headerNextPageNumberOffset:pageHeaderSize]))
}

// SetNextPageNumber は次の UNDO ページの PageNumber を設定する
func (p *Page) SetNextPageNumber(pn page.PageNumber) {
	binary.BigEndian.PutUint32(p.header[headerNextPageNumberOffset:pageHeaderSize], uint32(pn))
}

// FreeSpace はボディ内の空き容量を返す
func (p *Page) FreeSpace() int {
	return len(p.body) - int(p.UsedBytes())
}

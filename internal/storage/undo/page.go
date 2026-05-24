package undo

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const (
	headerUsedBytesOffset      = 0
	headerNextPageNumberOffset = 2
	pageHeaderSize             = 6 // UsedBytes(2) + NextPageNum(4)
)

type Page struct {
	header []byte
	body   []byte
}

// NewPage は既存の Undo ページを開く
func NewPage(pg page.Page) *Page {
	body := pg.Body()
	return &Page{
		header: body[:pageHeaderSize],
		body:   body[pageHeaderSize:],
	}
}

// CreatePage は新規 Undo ページを作成する
func CreatePage(pg page.Page) *Page {
	p := NewPage(pg)
	p.initialize()
	return p
}

// Record はボティ内の指定 offset のレコードを読み取る
func (p *Page) Record(offset int) []byte {
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

// FreeSpace はボディ内の空き容量を返す
func (p *Page) FreeSpace() int {
	return len(p.body) - int(p.UsedBytes())
}

// initialize は Undo ページを初期化する
func (p *Page) initialize() {
	binary.BigEndian.PutUint16(p.header[headerUsedBytesOffset:headerNextPageNumberOffset], 0) // usedBytes
	binary.BigEndian.PutUint32(p.header[headerNextPageNumberOffset:pageHeaderSize], 0)        // nextPageNumber
}

// append は Undo レコードをボディに追加する
//
// 空き不足の場合は false を返す
func (p *Page) append(record []byte) bool {
	used := int(p.UsedBytes())
	if used+len(record) > len(p.body) {
		return false
	}
	copy(p.body[used:], record)
	binary.BigEndian.PutUint16(p.header[headerUsedBytesOffset:headerNextPageNumberOffset], uint16(used+len(record)))
	return true
}

// setNextPageNumber は次の UNDO ページの PageNumber を設定する
func (p *Page) setNextPageNumber(pn page.PageNumber) {
	binary.BigEndian.PutUint32(p.header[headerNextPageNumberOffset:pageHeaderSize], uint32(pn))
}

// setUsedBytes は使用済みバイト数を設定する
func (p *Page) setUsedBytes(n uint16) {
	binary.BigEndian.PutUint16(p.header[headerUsedBytesOffset:headerNextPageNumberOffset], n)
}

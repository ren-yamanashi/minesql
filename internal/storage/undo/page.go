package undo

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const (
	headerUsedBytesOffset      = 0
	headerNextPageNumberOffset = 2
	pageHeaderSize             = 6 // UsedBytes(2) + NextPageNum(4)
)

type Page struct {
	// header / body は bufPage.Data().Body() への読み取りビュー (書き込みは bufPage の API 経由で行う必要がある)
	header  []byte
	body    []byte
	bufPage *buffer.Page
}

// NewPage は既存の Undo ページを開く
func NewPage(bufPage *buffer.Page) *Page {
	body := bufPage.Data().Body()
	return &Page{
		header:  body[:pageHeaderSize],
		body:    body[pageHeaderSize:],
		bufPage: bufPage,
	}
}

// CreatePage は新規 Undo ページを作成する
func CreatePage(bufPage *buffer.Page) *Page {
	p := NewPage(bufPage)
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
	var buf [pageHeaderSize]byte
	binary.BigEndian.PutUint16(buf[headerUsedBytesOffset:headerNextPageNumberOffset], 0)
	binary.BigEndian.PutUint32(buf[headerNextPageNumberOffset:pageHeaderSize], 0)
	p.bufPage.WriteBodyAt(0, buf[:])
}

// append は Undo レコードをボディに追加する
//
// 空き不足の場合は false を返す
func (p *Page) append(record []byte) bool {
	used := int(p.UsedBytes())
	if used+len(record) > len(p.body) {
		return false
	}
	p.bufPage.WriteBodyAt(pageHeaderSize+used, record)
	p.setUsedBytes(uint16(used + len(record)))
	return true
}

// setNextPageNumber は次の UNDO ページの PageNumber を設定する
func (p *Page) setNextPageNumber(pn page.PageNumber) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(pn))
	p.bufPage.WriteBodyAt(headerNextPageNumberOffset, buf[:])
}

// setUsedBytes は使用済みバイト数を設定する
func (p *Page) setUsedBytes(n uint16) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], n)
	p.bufPage.WriteBodyAt(headerUsedBytesOffset, buf[:])
}

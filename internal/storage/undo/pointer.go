package undo

import (
	"encoding/binary"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const (
	pageNumberOffset = 0
	offsetOffset     = 4
	PointerSize      = 6 // PageNumber(4) + Offset(2)
)

var ErrInvalidPointerData = errors.New("undo: data size must be at least 6 bytes to decode pointer")

type Pointer struct {
	pageNumber page.PageNumber // Undo ページのページ番号
	offset     uint16          // Undo ページ内のバイトオフセット
}

func NewPointer(pageNum page.PageNumber, offset uint16) Pointer {
	return Pointer{pageNumber: pageNum, offset: offset}
}

func (p Pointer) Encode() []byte {
	buf := make([]byte, PointerSize)
	binary.BigEndian.PutUint32(buf[pageNumberOffset:offsetOffset], uint32(p.pageNumber))
	binary.BigEndian.PutUint16(buf[offsetOffset:PointerSize], p.offset)
	return buf
}

func (p Pointer) IsNull() bool {
	return p.pageNumber == 0xFFFFFFFF && p.offset == 0xFFFF
}

func DecodePointer(data []byte) (Pointer, error) {
	if len(data) < PointerSize {
		return NullPointer(), ErrInvalidPointerData
	}
	return Pointer{
		pageNumber: page.PageNumber(binary.BigEndian.Uint32(data[pageNumberOffset:offsetOffset])),
		offset:     binary.BigEndian.Uint16(data[offsetOffset:PointerSize]),
	}, nil
}

// NullPointer は前バージョンが存在しないことを示す Pointer
func NullPointer() Pointer {
	return Pointer{pageNumber: 0xFFFFFFFF, offset: 0xFFFF}
}

package undo

import (
	"encoding/binary"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const (
	pageNumberOffset = 0
	dataOffsetOffset = 4
	PointerSize      = 6 // PageNumber(4) + Offset(2)
)

var (
	// NullPointer は前バージョンが存在しないことを示す
	NullPointer           = Pointer{pageNumber: 0xFFFFFFFF, offset: 0xFFFF}
	ErrInvalidPointerData = errors.New("undo: data size must be at least 6 bytes to decode pointer")
)

// Pointer は Undo ログレコードの位置を指すポインタ
type Pointer struct {
	pageNumber page.PageNumber // Undo ページのページ番号
	offset     uint16          // Undo ページ内のバイトオフセット
}

func NewPointer(pageNum page.PageNumber, offset uint16) Pointer {
	return Pointer{pageNumber: pageNum, offset: offset}
}

// Encode は Pointer を 6 バイトのバイト列にエンコードする
func (p Pointer) Encode() []byte {
	buf := make([]byte, PointerSize)
	binary.BigEndian.PutUint32(buf[pageNumberOffset:dataOffsetOffset], uint32(p.pageNumber))
	binary.BigEndian.PutUint16(buf[dataOffsetOffset:PointerSize], p.offset)
	return buf
}

// isNull は前バージョンが存在しないかどうかを返す
func (p Pointer) isNull() bool {
	return p == NullPointer
}

// DecodePointer はバイト列から Pointer をデコードする
func DecodePointer(data []byte) (Pointer, error) {
	if len(data) < PointerSize {
		return NullPointer, ErrInvalidPointerData
	}
	return Pointer{
		pageNumber: page.PageNumber(binary.BigEndian.Uint32(data[pageNumberOffset:dataOffsetOffset])),
		offset:     binary.BigEndian.Uint16(data[dataOffsetOffset:PointerSize]),
	}, nil
}

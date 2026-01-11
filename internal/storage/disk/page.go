package disk

import (
	"encoding/binary"
	"fmt"
)

const PAGE_SIZE = 4096

var (
	ErrInvalidDataSize error  = fmt.Errorf("data size must be %d bytes", PAGE_SIZE)
	INVALID_PAGE_ID    PageId = NewPageId(0xFFFFFFFF, 0xFFFFFFFF)
)

type (
	Page       [PAGE_SIZE]uint8
	FileId     uint32
	PageNumber uint32
)

type PageId struct {
	// ディスクファイルの識別子
	FileId FileId
	// ファイル内のページ番号
	PageNumber PageNumber
}

func NewPageId(fileId FileId, pageNumber PageNumber) PageId {
	return PageId{
		FileId:     fileId,
		PageNumber: pageNumber,
	}
}

func (p *PageId) Equals(other PageId) bool {
	return p.FileId == other.FileId && p.PageNumber == other.PageNumber
}

func (p *PageId) IsInvalid() bool {
	return p.Equals(INVALID_PAGE_ID)
}

// PageId をバイト列に変換
// 先頭4バイトに FileId、次の4バイトに PageNumber が格納される
func (p *PageId) ToBytes() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(p.FileId))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(p.PageNumber))
	return buf
}

// バイト列から PageId を復元
func PageIdFromBytes(data []byte) PageId {
	if len(data) != 8 {
		panic("data size must be 8 bytes to convert to PageId")
	}
	return PageId{
		FileId:     FileId(binary.LittleEndian.Uint32(data[0:4])),
		PageNumber: PageNumber(binary.LittleEndian.Uint32(data[4:8])),
	}
}

// PageId を指定位置に書き込む
func (p PageId) WriteTo(data []byte, offset int) {
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(p.FileId))
	binary.LittleEndian.PutUint32(data[offset+4:offset+8], uint32(p.PageNumber))
}

// 指定位置から PageId を読み取る
func ReadPageIdFrom(data []byte, offset int) PageId {
	return PageId{
		FileId:     FileId(binary.LittleEndian.Uint32(data[offset : offset+4])),
		PageNumber: PageNumber(binary.LittleEndian.Uint32(data[offset+4 : offset+8])),
	}
}
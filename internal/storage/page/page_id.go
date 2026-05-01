package page

import (
	"encoding/binary"
	"fmt"
)

const (
	MaxFileId     = 0xFFFFFFFF
	MaxPageNumber = 0xFFFFFFFF
)

var InvalidPageId = NewPageId(MaxFileId, MaxPageNumber)

type (
	FileId     uint32
	PageNumber uint32
)

// PageId は全体でページを一意に特定するための識別子
//
// レイアウト:
//   - FileId: 先頭 4 バイト
//   - PageNumber: 次の 4 バイト
type PageId struct {
	FileId     FileId
	PageNumber PageNumber
}

func NewPageId(fileId FileId, pageNumber PageNumber) PageId {
	return PageId{
		FileId:     fileId,
		PageNumber: pageNumber,
	}
}

// IsInvalid はこの PageId が無効かどうかを判定する
func (id PageId) IsInvalid() bool {
	return id == InvalidPageId
}

// ToBytes は PageId をバイト列に変換する
func (id PageId) ToBytes() []byte {
	data := make([]byte, 8)
	id.WriteTo(data, 0)
	return data
}

// WriteTo は PageId を指定位置に書き込む
//   - data: データ全体
//   - offset: 書き込み開始位置
func (id PageId) WriteTo(data []byte, offset int) {
	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(id.FileId))
	binary.BigEndian.PutUint32(data[offset+4:offset+8], uint32(id.PageNumber))
}

// ReadPageId は PageId を指定位置から読み込む
//   - data: データ全体
//   - offset: PageId が格納されている位置
func ReadPageId(data []byte, offset int) PageId {
	fileId := binary.BigEndian.Uint32(data[offset : offset+4])
	pageNumber := binary.BigEndian.Uint32(data[offset+4 : offset+8])
	return NewPageId(FileId(fileId), PageNumber(pageNumber))
}

// RestorePageId はバイト列から PageId を復元する
//   - data: PageId を表す 8 バイトのバイト列
func RestorePageId(data []byte) (PageId, error) {
	size := len(data)
	if size != 8 {
		return InvalidPageId, fmt.Errorf("page id must be 8 bytes, got %d", size)
	}
	return ReadPageId(data, 0), nil
}

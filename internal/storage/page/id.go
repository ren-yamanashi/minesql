package page

import (
	"encoding/binary"
	"fmt"
)

const (
	FileIdSize    = 4
	IdSize        = FileIdSize * 2
	MaxFileId     = 0xFFFFFFFF
	MaxPageNumber = 0xFFFFFFFF
)

var InvalidId = NewId(MaxFileId, MaxPageNumber)

type (
	FileId     uint32
	PageNumber uint32
)

// Id は全体でページを一意に特定するための識別子 (PageId)
//   - FileId: 先頭 4 バイト
//   - PageNumber: 次の 4 バイト
type Id struct {
	FileId     FileId
	PageNumber PageNumber
}

func NewId(fileId FileId, pageNumber PageNumber) Id {
	return Id{
		FileId:     fileId,
		PageNumber: pageNumber,
	}
}

// IsInvalid はこの Id が無効かどうかを判定する
func (id Id) IsInvalid() bool {
	return id == InvalidId
}

// ToBytes は Id をバイト列に変換する
func (id Id) ToBytes() []byte {
	data := make([]byte, IdSize)
	id.WriteTo(data, 0)
	return data
}

// WriteTo は Id を指定位置に書き込む
//   - data: データ全体
//   - offset: 書き込み開始位置
func (id Id) WriteTo(data []byte, offset int) {
	binary.BigEndian.PutUint32(data[offset:offset+FileIdSize], uint32(id.FileId))
	binary.BigEndian.PutUint32(data[offset+FileIdSize:offset+IdSize], uint32(id.PageNumber))
}

// ReadId は Id を指定位置から読み込む
//   - data: データ全体
//   - offset: Id が格納されている位置
func ReadId(data []byte, offset int) Id {
	fileId := binary.BigEndian.Uint32(data[offset : offset+FileIdSize])
	pageNumber := binary.BigEndian.Uint32(data[offset+FileIdSize : offset+IdSize])
	return NewId(FileId(fileId), PageNumber(pageNumber))
}

// RestoreId はバイト列から Id を復元する
//   - data: Id を表す 8 バイトのバイト列
func RestoreId(data []byte) (Id, error) {
	if len(data) != IdSize {
		return InvalidId, fmt.Errorf("page id must be %d bytes, got %d", IdSize, len(data))
	}
	return ReadId(data, 0), nil
}

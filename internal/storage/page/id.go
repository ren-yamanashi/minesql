package page

import (
	"encoding/binary"
	"fmt"
)

const (
	FileIdSize = 4
	IdSize     = FileIdSize * 2
)

const (
	MaxFileId     FileId     = 0xFFFFFFFF
	MaxPageNumber PageNumber = 0xFFFFFFFF
)

type (
	FileId     uint32
	PageNumber uint32
)

// Id は全体でページを一意に特定するための識別子 (PageId)
//   - FileId: 先頭 4 バイト
//   - PageNumber: 次の 4 バイト
type Id struct {
	fileId     FileId
	pageNumber PageNumber
}

func NewId(fileId FileId, pageNumber PageNumber) Id {
	return Id{
		fileId:     fileId,
		pageNumber: pageNumber,
	}
}

func (id Id) FileId() FileId         { return id.fileId }
func (id Id) PageNumber() PageNumber { return id.pageNumber }
func (id Id) IsInvalid() bool        { return id == InvalidId() }

func (id Id) Bytes() []byte {
	data := make([]byte, IdSize)
	id.WriteAt(data, 0)
	return data
}

// WriteAt は Id を指定位置に書き込む
//   - data: データ全体
//   - offset: 書き込み開始位置
func (id Id) WriteAt(data []byte, offset int) {
	binary.BigEndian.PutUint32(data[offset:offset+FileIdSize], uint32(id.fileId))
	binary.BigEndian.PutUint32(data[offset+FileIdSize:offset+IdSize], uint32(id.pageNumber))
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
		return InvalidId(), fmt.Errorf("page id must be %d bytes, got %d", IdSize, len(data))
	}
	return ReadId(data, 0), nil
}

// InvalidId は不正値を示す Id
func InvalidId() Id {
	return NewId(MaxFileId, MaxPageNumber)
}

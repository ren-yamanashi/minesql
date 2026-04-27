package page

import (
	"encoding/binary"
	"fmt"
)

const MaxFileId = 0xFFFFFFFF
const MaxPageNumber = 0xFFFFFFFF

var InvalidId = NewId(MaxFileId, MaxPageNumber)

// Id は全体でページを一意に特定するための識別子
//
// レイアウト:
//   - FileId: 先頭 4 バイト
//   - PageNumber: 次の 4 バイト
type Id struct {
	FileId     uint32
	PageNumber uint32
}

func NewId(fileId uint32, pageNum uint32) Id {
	return Id{
		FileId:     fileId,
		PageNumber: pageNum,
	}
}

func (id Id) IsInvalid() bool {
	return id == InvalidId
}

func (id Id) ToBytes() []byte {
	data := make([]byte, 8)
	id.WriteTo(data, 0)
	return data
}

// WriteTo は Id を指定位置に書き込む
//   - data: データ全体
//   - offset: 書き込み開始位置
func (id Id) WriteTo(data []byte, offset int) {
	binary.BigEndian.PutUint32(data[offset:offset+4], id.FileId)
	binary.BigEndian.PutUint32(data[offset+4:offset+8], id.PageNumber)
}

// ReadId は Id を指定位置から読み込む
//   - data: データ全体
//   - offset: Id が格納されている位置
func ReadId(data []byte, offset int) Id {
	fileId := binary.BigEndian.Uint32(data[offset : offset+4])
	pageNum := binary.BigEndian.Uint32(data[offset+4 : offset+8])
	return NewId(fileId, pageNum)
}

// RestoreId はバイト列から Id を復元する
//   - data: Id を表す 8 バイトのバイト列 (先頭4バイトに FileId、次の4バイトに PageNumber が格納されている必要がある)
func RestoreId(data []byte) (Id, error) {
	if len(data) != 8 {
		return InvalidId, fmt.Errorf("page id must be 8 bytes, got %d", len(data))
	}
	return ReadId(data, 0), nil
}

package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type TableRecord struct {
	FileId   page.FileId // テーブルの FileId
	Name     string      // テーブル名
	NumOfCol int         // カラム数
}

func NewTableRecord(fileId page.FileId, name string, numOfCol int) TableRecord {
	return TableRecord{
		FileId:   fileId,
		Name:     name,
		NumOfCol: numOfCol,
	}
}

// encode は node.Record にエンコードする
func (tr TableRecord) encode() node.Record {
	// key = name
	var key []byte
	encode.Encode([][]byte{[]byte(tr.Name)}, &key)

	// nonKey = fileId + numOfCol
	var nonKey []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(tr.FileId))
	numOfCol := binary.BigEndian.AppendUint32(nil, uint32(tr.NumOfCol))
	encode.Encode([][]byte{fileId, numOfCol}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeTableRecord は node.Record から tableRecord にデコードする
func decodeTableRecord(record node.Record) TableRecord {
	// key = [name]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	name := string(key[0])

	// nonKey = [fileId, numOfCol]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	fileId := page.FileId(binary.BigEndian.Uint32(nonKey[0]))
	numOfCol := int(binary.BigEndian.Uint32(nonKey[1]))

	return NewTableRecord(fileId, name, numOfCol)
}

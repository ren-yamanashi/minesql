package dictionary

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type tableRecord struct {
	fileId   page.FileId // テーブルの FileId
	name     string      // テーブル名
	numOfCol int         // カラム数
}

func newTableRecord(fileId page.FileId, name string, numOfCol int) tableRecord {
	return tableRecord{
		fileId:   fileId,
		name:     name,
		numOfCol: numOfCol,
	}
}

// encode は node.Record にエンコードする
func (tr tableRecord) encode() node.Record {
	// key = fileId
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(tr.fileId))
	encode.Encode([][]byte{fileId}, &key)

	// nonKey = name + numOfCol
	var nonKey []byte
	numOfCol := binary.BigEndian.AppendUint32(nil, uint32(tr.numOfCol))
	encode.Encode([][]byte{[]byte(tr.name), numOfCol}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeTableRecord は node.Record から tableRecord にデコードする
func decodeTableRecord(record node.Record) tableRecord {
	// key = [fileId]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))

	// nonKey = [name, numOfCol]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	name := string(nonKey[0])
	numOfCol := int(binary.BigEndian.Uint32(nonKey[1]))

	return newTableRecord(fileId, name, numOfCol)
}

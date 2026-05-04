package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type (
	IndexId   uint32
	IndexType byte
)

const (
	IndexTypePrimary   IndexType = 1
	IndexTypeUnique    IndexType = 2
	IndexTypeNonUnique IndexType = 3
)

type IndexRecord struct {
	FileId    page.FileId // インデックスが属するテーブルの FileId
	IndexId   IndexId     // インデックス ID
	Name      string      // インデックス名
	IndexType IndexType   // インデックス種類
	NumOfCol  int         // インデックスを構成するカラム数
}

func NewIndexRecord(fileId page.FileId, indexId IndexId, name string, indexType IndexType, numOfCol int) IndexRecord {
	return IndexRecord{
		FileId:    fileId,
		IndexId:   indexId,
		Name:      name,
		IndexType: indexType,
		NumOfCol:  numOfCol,
	}
}

// encode は node.Record にエンコードする
func (ir IndexRecord) encode() node.Record {
	// key = fileId + name
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(ir.FileId))
	encode.Encode([][]byte{fileId, []byte(ir.Name)}, &key)

	// nonKey = indexId + indexType + numOfCol
	var nonKey []byte
	indexId := binary.BigEndian.AppendUint32(nil, uint32(ir.IndexId))
	numOfCol := binary.BigEndian.AppendUint32(nil, uint32(ir.NumOfCol))
	encode.Encode([][]byte{indexId, {byte(ir.IndexType)}, numOfCol}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeIndexRecord は node.Record から IndexRecord にデコードする
func decodeIndexRecord(record node.Record) IndexRecord {
	// key = [fileId, name]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	name := string(key[1])

	// nonKey = [indexId, indexType, numOfCol]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	indexId := IndexId(binary.BigEndian.Uint32(nonKey[0]))
	indexType := IndexType(nonKey[1][0])
	numOfCol := int(binary.BigEndian.Uint32(nonKey[2]))

	return NewIndexRecord(fileId, indexId, name, indexType, numOfCol)
}

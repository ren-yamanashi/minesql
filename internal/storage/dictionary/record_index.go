package dictionary

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

type indexRecord struct {
	fileId    page.FileId // インデックスが属するテーブルの FileId
	indexId   IndexId     // インデックス ID
	name      string      // インデックス名
	indexType IndexType   // インデックス種類
}

func newIndexRecord(fileId page.FileId, indexId IndexId, name string, indexType IndexType) indexRecord {
	return indexRecord{
		fileId:    fileId,
		indexId:   indexId,
		name:      name,
		indexType: indexType,
	}
}

// encode は node.Record にエンコードする
func (ir indexRecord) encode() node.Record {
	// key = fileId + indexId
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(ir.fileId))
	indexId := binary.BigEndian.AppendUint32(nil, uint32(ir.indexId))
	encode.Encode([][]byte{fileId, indexId}, &key)

	// nonKey = name + indexType
	var nonKey []byte
	encode.Encode([][]byte{[]byte(ir.name), {byte(ir.indexType)}}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeIndexRecord は node.Record から indexRecord にデコードする
func decodeIndexRecord(record node.Record) indexRecord {
	// key = [fileId, indexId]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	indexId := IndexId(binary.BigEndian.Uint32(key[1]))

	// nonKey = [name, indexType]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	name := string(nonKey[0])
	indexType := IndexType(nonKey[1][0])

	return newIndexRecord(fileId, indexId, name, indexType)
}

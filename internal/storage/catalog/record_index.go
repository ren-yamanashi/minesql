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
	PrimaryIndexName             = "PRIMARY"
)

type IndexRecord struct {
	FileId     page.FileId // インデックスが属するテーブルの FileId
	IndexId    IndexId     // インデックス ID
	Name       string      // インデックス名
	IndexType  IndexType   // インデックス種類
	NumOfCol   int         // インデックスを構成するカラム数
	MetaPageId page.PageId // セカンダリ or プライマリインデックスの B+Tree メタページ ID
}

func newIndexRecord(fileId page.FileId, name string, indexId IndexId, indexType IndexType, numOfCol int, metaPageId page.PageId) IndexRecord {
	return IndexRecord{
		FileId:     fileId,
		IndexId:    indexId,
		Name:       name,
		IndexType:  indexType,
		NumOfCol:   numOfCol,
		MetaPageId: metaPageId,
	}
}

// encode は node.Record にエンコードする
func (ir IndexRecord) encode() node.Record {
	// key = fileId + name
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(ir.FileId))
	encode.Encode([][]byte{fileId, []byte(ir.Name)}, &key)

	// nonKey = indexId + indexType + numOfCol + metaPageId
	var nonKey []byte
	indexId := binary.BigEndian.AppendUint32(nil, uint32(ir.IndexId))
	numOfCol := binary.BigEndian.AppendUint32(nil, uint32(ir.NumOfCol))
	metaPageIdBytes := ir.MetaPageId.ToBytes()
	encode.Encode([][]byte{indexId, {byte(ir.IndexType)}, numOfCol, metaPageIdBytes}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeIndexRecord は node.Record から IndexRecord にデコードする
func decodeIndexRecord(record node.Record) IndexRecord {
	// key = [fileId, name]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	name := string(key[1])

	// nonKey = [indexId, indexType, numOfCol, metaPageId]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	indexId := IndexId(binary.BigEndian.Uint32(nonKey[0]))
	indexType := IndexType(nonKey[1][0])
	numOfCol := int(binary.BigEndian.Uint32(nonKey[2]))
	metaPageId := page.ReadPageId(nonKey[3], 0)

	return newIndexRecord(fileId, name, indexId, indexType, numOfCol, metaPageId)
}

package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type (
	IndexId   uint32
	IndexType int
)

const PrimaryIndexName = "PRIMARY"

const (
	indexTypeUnknown IndexType = iota
	IndexTypePrimary
	IndexTypeUnique
	IndexTypeNonUnique
)

type IndexRecord struct {
	fileId      page.FileId // インデックスが属するテーブルの FileId
	indexId     IndexId     // インデックス ID
	name        string      // インデックス名
	indexType   IndexType   // インデックス種類
	columnCount int         // インデックスを構成するカラム数
	metaPageId  page.Id     // セカンダリ or プライマリインデックスの B+Tree メタページ ID
}

func NewIndexRecord(
	fileId page.FileId,
	indexId IndexId,
	name string,
	indexType IndexType,
	columnCount int,
	metaPageId page.Id,
) IndexRecord {
	return IndexRecord{
		fileId:      fileId,
		indexId:     indexId,
		name:        name,
		indexType:   indexType,
		columnCount: columnCount,
		metaPageId:  metaPageId,
	}
}

func (ir IndexRecord) FileId() page.FileId  { return ir.fileId }
func (ir IndexRecord) IndexId() IndexId     { return ir.indexId }
func (ir IndexRecord) Name() string         { return ir.name }
func (ir IndexRecord) IndexType() IndexType { return ir.indexType }
func (ir IndexRecord) ColumnCount() int     { return ir.columnCount }
func (ir IndexRecord) MetaPageId() page.Id  { return ir.metaPageId }

func (ir IndexRecord) Encode() btree.Record {
	// key = fileId + name
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(ir.fileId))
	encode.Encode([][]byte{fileId, []byte(ir.name)}, &key)

	// nonKey = indexId + indexType + numOfCol + metaPageId
	var nonKey []byte
	indexId := binary.BigEndian.AppendUint32(nil, uint32(ir.indexId))
	numOfCol := binary.BigEndian.AppendUint32(nil, uint32(ir.columnCount))
	metaPageIdBytes := ir.metaPageId.ToBytes()
	encode.Encode([][]byte{indexId, {byte(ir.indexType)}, numOfCol, metaPageIdBytes}, &nonKey)

	return btree.NewRecord(nil, key, nonKey)
}

func DecodeIndexRecord(record btree.Record) IndexRecord {
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
	metaPageId := page.ReadId(nonKey[3], 0)

	return NewIndexRecord(fileId, indexId, name, indexType, numOfCol, metaPageId)
}

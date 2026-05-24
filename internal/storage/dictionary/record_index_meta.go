package dictionary

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

type IndexMetaRecord struct {
	fileId      page.FileId // インデックスが属するテーブルの FileId
	indexId     IndexId     // インデックス ID
	name        string      // インデックス名
	indexType   IndexType   // インデックス種類
	columnCount int         // インデックスを構成するカラム数
	metaPageId  page.Id     // セカンダリ or プライマリインデックスの B+Tree メタページ ID
}

func NewIndexMetaRecord(
	fileId page.FileId,
	indexId IndexId,
	name string,
	indexType IndexType,
	columnCount int,
	metaPageId page.Id,
) IndexMetaRecord {
	return IndexMetaRecord{
		fileId:      fileId,
		indexId:     indexId,
		name:        name,
		indexType:   indexType,
		columnCount: columnCount,
		metaPageId:  metaPageId,
	}
}

func (ir IndexMetaRecord) FileId() page.FileId  { return ir.fileId }
func (ir IndexMetaRecord) IndexId() IndexId     { return ir.indexId }
func (ir IndexMetaRecord) Name() string         { return ir.name }
func (ir IndexMetaRecord) IndexType() IndexType { return ir.indexType }
func (ir IndexMetaRecord) ColumnCount() int     { return ir.columnCount }
func (ir IndexMetaRecord) MetaPageId() page.Id  { return ir.metaPageId }

func (ir IndexMetaRecord) Encode() btree.Record {
	// key = fileId + name
	fileId := binary.BigEndian.AppendUint32(nil, uint32(ir.fileId))
	key := encode.Encode(nil, [][]byte{fileId, []byte(ir.name)})

	// nonKey = indexId + indexType + numOfCol + metaPageId
	indexId := binary.BigEndian.AppendUint32(nil, uint32(ir.indexId))
	numOfCol := binary.BigEndian.AppendUint32(nil, uint32(ir.columnCount))
	metaPageIdBytes := ir.metaPageId.Bytes()
	nonKey := encode.Encode(nil, [][]byte{indexId, {byte(ir.indexType)}, numOfCol, metaPageIdBytes})

	return btree.NewRecord(nil, key, nonKey)
}

func DecodeIndexMetaRecord(record btree.Record) (IndexMetaRecord, error) {
	// key = [fileId, name]
	key, err := encode.Decode(record.Key())
	if err != nil {
		return IndexMetaRecord{}, err
	}
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	name := string(key[1])

	// nonKey = [indexId, indexType, numOfCol, metaPageId]
	nonKey, err := encode.Decode(record.NonKey())
	if err != nil {
		return IndexMetaRecord{}, err
	}
	indexId := IndexId(binary.BigEndian.Uint32(nonKey[0]))
	indexType := IndexType(nonKey[1][0])
	numOfCol := int(binary.BigEndian.Uint32(nonKey[2]))
	metaPageId := page.ReadId(nonKey[3], 0)

	return NewIndexMetaRecord(fileId, indexId, name, indexType, numOfCol, metaPageId), nil
}

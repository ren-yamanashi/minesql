package dictionary

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type TableMetaRecord struct {
	name        string  // テーブル名
	metaPageId  page.Id // プライマリインデックスの B+Tree メタページ ID
	columnCount int     // カラム数
}

func NewTableMetaRecord(name string, metaPageId page.Id, numOfCol int) TableMetaRecord {
	return TableMetaRecord{
		name:        name,
		metaPageId:  metaPageId,
		columnCount: numOfCol,
	}
}

func (tr TableMetaRecord) Name() string        { return tr.name }
func (tr TableMetaRecord) MetaPageId() page.Id { return tr.metaPageId }
func (tr TableMetaRecord) ColumnCount() int    { return tr.columnCount }

func (tr TableMetaRecord) Encode() btree.Record {
	// key = name
	key := encode.Encode(nil, [][]byte{[]byte(tr.name)})

	// nonKey = metaPageId + numOfCol
	metaPageIdBytes := tr.metaPageId.Bytes()
	numOfCol := binary.BigEndian.AppendUint32(nil, uint32(tr.columnCount))
	nonKey := encode.Encode(nil, [][]byte{metaPageIdBytes, numOfCol})

	return btree.NewRecord(nil, key, nonKey)
}

func DecodeTableMetaRecord(record btree.Record) (TableMetaRecord, error) {
	// key = [name]
	key, err := encode.Decode(record.Key())
	if err != nil {
		return TableMetaRecord{}, err
	}
	name := string(key[0])

	// nonKey = [metaPageId, numOfCol]
	nonKey, err := encode.Decode(record.NonKey())
	if err != nil {
		return TableMetaRecord{}, err
	}
	metaPageId := page.ReadId(nonKey[0], 0)
	numOfCol := int(binary.BigEndian.Uint32(nonKey[1]))

	return NewTableMetaRecord(name, metaPageId, numOfCol), nil
}

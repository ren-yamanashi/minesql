package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type TableRecord struct {
	name        string  // テーブル名
	metaPageId  page.Id // プライマリインデックスの B+Tree メタページ ID
	columnCount int     // カラム数
}

func newTableRecord(name string, metaPageId page.Id, numOfCol int) TableRecord {
	return TableRecord{
		name:        name,
		metaPageId:  metaPageId,
		columnCount: numOfCol,
	}
}

func (tr TableRecord) Name() string        { return tr.name }
func (tr TableRecord) MetaPageId() page.Id { return tr.metaPageId }
func (tr TableRecord) ColumnCount() int    { return tr.columnCount }

// encode は btree.Record にエンコードする
func (tr TableRecord) encode() btree.Record {
	// key = name
	var key []byte
	encode.Encode([][]byte{[]byte(tr.name)}, &key)

	// nonKey = metaPageId + numOfCol
	var nonKey []byte
	metaPageIdBytes := tr.metaPageId.ToBytes()
	numOfCol := binary.BigEndian.AppendUint32(nil, uint32(tr.columnCount))
	encode.Encode([][]byte{metaPageIdBytes, numOfCol}, &nonKey)

	return btree.NewRecord(nil, key, nonKey)
}

// decodeTableRecord は btree.Record から TableRecord にデコードする
func decodeTableRecord(record btree.Record) TableRecord {
	// key = [name]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	name := string(key[0])

	// nonKey = [metaPageId, numOfCol]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	metaPageId := page.ReadId(nonKey[0], 0)
	numOfCol := int(binary.BigEndian.Uint32(nonKey[1]))

	return newTableRecord(name, metaPageId, numOfCol)
}

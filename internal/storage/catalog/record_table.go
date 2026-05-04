package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type TableRecord struct {
	Name       string      // テーブル名
	MetaPageId page.PageId // プライマリインデックスの B+Tree メタページ ID
	NumOfCol   int         // カラム数
}

func newTableRecord(name string, metaPageId page.PageId, numOfCol int) TableRecord {
	return TableRecord{
		Name:       name,
		MetaPageId: metaPageId,
		NumOfCol:   numOfCol,
	}
}

// encode は node.Record にエンコードする
func (tr TableRecord) encode() node.Record {
	// key = name
	var key []byte
	encode.Encode([][]byte{[]byte(tr.Name)}, &key)

	// nonKey = metaPageId + numOfCol
	var nonKey []byte
	metaPageIdBytes := tr.MetaPageId.ToBytes()
	numOfCol := binary.BigEndian.AppendUint32(nil, uint32(tr.NumOfCol))
	encode.Encode([][]byte{metaPageIdBytes, numOfCol}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeTableRecord は node.Record から TableRecord にデコードする
func decodeTableRecord(record node.Record) TableRecord {
	// key = [name]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	name := string(key[0])

	// nonKey = [metaPageId, numOfCol]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	metaPageId := page.ReadPageId(nonKey[0], 0)
	numOfCol := int(binary.BigEndian.Uint32(nonKey[1]))

	return newTableRecord(name, metaPageId, numOfCol)
}

package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type indexKeyColRecord struct {
	indexId IndexId
	name    string // カラム名
	pos     int    // テーブル上のカラム位置
}

func newIndexKeyColRecord(indexId IndexId, name string, pos int) indexKeyColRecord {
	return indexKeyColRecord{
		indexId: indexId,
		name:    name,
		pos:     pos,
	}
}

// encode は node.Record にエンコードする
func (kcr indexKeyColRecord) encode() node.Record {
	// key = indexId + name
	var key []byte
	indexId := binary.BigEndian.AppendUint32(nil, uint32(kcr.indexId))
	encode.Encode([][]byte{indexId, []byte(kcr.name)}, &key)

	// nonKey = pos
	var nonKey []byte
	pos := binary.BigEndian.AppendUint32(nil, uint32(kcr.pos))
	encode.Encode([][]byte{pos}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeIndexKeyColRecord は node.Record から indexKeyColRecord にデコードする
func decodeIndexKeyColRecord(record node.Record) indexKeyColRecord {
	// key = [indexId, name]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	indexId := IndexId(binary.BigEndian.Uint32(key[0]))
	name := string(key[1])

	// nonKey = [pos]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	pos := int(binary.BigEndian.Uint32(nonKey[0]))

	return newIndexKeyColRecord(indexId, name, pos)
}

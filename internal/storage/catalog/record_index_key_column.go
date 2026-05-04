package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type IndexKeyColRecord struct {
	IndexId IndexId
	Name    string // カラム名
	Pos     int    // インデックス上のカラム位置
}

func newIndexKeyColRecord(indexId IndexId, name string, pos int) IndexKeyColRecord {
	return IndexKeyColRecord{
		IndexId: indexId,
		Name:    name,
		Pos:     pos,
	}
}

// encode は node.Record にエンコードする
func (kcr IndexKeyColRecord) encode() node.Record {
	// key = indexId + name
	var key []byte
	indexId := binary.BigEndian.AppendUint32(nil, uint32(kcr.IndexId))
	encode.Encode([][]byte{indexId, []byte(kcr.Name)}, &key)

	// nonKey = pos
	var nonKey []byte
	pos := binary.BigEndian.AppendUint32(nil, uint32(kcr.Pos))
	encode.Encode([][]byte{pos}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeIndexKeyColRecord は node.Record から indexKeyColRecord にデコードする
func decodeIndexKeyColRecord(record node.Record) IndexKeyColRecord {
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

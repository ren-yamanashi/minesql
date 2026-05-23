package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type IndexKeyColumnRecord struct {
	indexId  IndexId
	name     string // カラム名
	position int    // インデックス上のカラム位置
}

func newIndexKeyColumnRecord(indexId IndexId, name string, pos int) IndexKeyColumnRecord {
	return IndexKeyColumnRecord{
		indexId:  indexId,
		name:     name,
		position: pos,
	}
}

func (kcr IndexKeyColumnRecord) IndexId() IndexId { return kcr.indexId }
func (kcr IndexKeyColumnRecord) Name() string     { return kcr.name }
func (kcr IndexKeyColumnRecord) Position() int    { return kcr.position }

// encode は btree.Record にエンコードする
func (kcr IndexKeyColumnRecord) encode() btree.Record {
	// key = indexId + name
	var key []byte
	indexId := binary.BigEndian.AppendUint32(nil, uint32(kcr.indexId))
	encode.Encode([][]byte{indexId, []byte(kcr.name)}, &key)

	// nonKey = pos
	var nonKey []byte
	pos := binary.BigEndian.AppendUint32(nil, uint32(kcr.position))
	encode.Encode([][]byte{pos}, &nonKey)

	return btree.NewRecord(nil, key, nonKey)
}

// decodeIndexKeyColumnRecord は btree.Record から indexKeyColRecord にデコードする
func decodeIndexKeyColumnRecord(record btree.Record) IndexKeyColumnRecord {
	// key = [indexId, name]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	indexId := IndexId(binary.BigEndian.Uint32(key[0]))
	name := string(key[1])

	// nonKey = [pos]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	pos := int(binary.BigEndian.Uint32(nonKey[0]))

	return newIndexKeyColumnRecord(indexId, name, pos)
}

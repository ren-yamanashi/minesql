package dictionary

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type IndexKeyColumnMetaRecord struct {
	indexId  IndexId
	name     string // カラム名
	position int    // インデックス上のカラム位置
}

func NewIndexKeyColumnMetaRecord(indexId IndexId, name string, pos int) IndexKeyColumnMetaRecord {
	return IndexKeyColumnMetaRecord{
		indexId:  indexId,
		name:     name,
		position: pos,
	}
}

func (kcr IndexKeyColumnMetaRecord) IndexId() IndexId { return kcr.indexId }
func (kcr IndexKeyColumnMetaRecord) Name() string     { return kcr.name }
func (kcr IndexKeyColumnMetaRecord) Position() int    { return kcr.position }

func (kcr IndexKeyColumnMetaRecord) Encode() btree.Record {
	// key = indexId + name
	indexId := binary.BigEndian.AppendUint32(nil, uint32(kcr.indexId))
	key := encode.Encode(nil, [][]byte{indexId, []byte(kcr.name)})

	// nonKey = pos
	pos := binary.BigEndian.AppendUint32(nil, uint32(kcr.position))
	nonKey := encode.Encode(nil, [][]byte{pos})

	return btree.NewRecord(nil, key, nonKey)
}

func DecodeIndexKeyColumnMetaRecord(record btree.Record) (IndexKeyColumnMetaRecord, error) {
	// key = [indexId, name]
	key, err := encode.Decode(record.Key())
	if err != nil {
		return IndexKeyColumnMetaRecord{}, err
	}
	indexId := IndexId(binary.BigEndian.Uint32(key[0]))
	name := string(key[1])

	// nonKey = [pos]
	nonKey, err := encode.Decode(record.NonKey())
	if err != nil {
		return IndexKeyColumnMetaRecord{}, err
	}
	pos := int(binary.BigEndian.Uint32(nonKey[0]))

	return NewIndexKeyColumnMetaRecord(indexId, name, pos), nil
}

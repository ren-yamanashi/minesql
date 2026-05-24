package dictionary

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ColumnMetaRecord struct {
	fileId   page.FileId // カラムが属するテーブルの FileId
	name     string      // カラム名
	position int         // テーブル上のカラム位置
}

func NewColumnMetaRecord(fileId page.FileId, name string, pos int) ColumnMetaRecord {
	return ColumnMetaRecord{
		fileId:   fileId,
		name:     name,
		position: pos,
	}
}

func (cr ColumnMetaRecord) FileId() page.FileId { return cr.fileId }
func (cr ColumnMetaRecord) Name() string        { return cr.name }
func (cr ColumnMetaRecord) Position() int       { return cr.position }

func (cr ColumnMetaRecord) Encode() btree.Record {
	// key = fileId + name
	fileId := binary.BigEndian.AppendUint32(nil, uint32(cr.fileId))
	key := encode.Encode(nil, [][]byte{fileId, []byte(cr.name)})

	// nonKey = pos
	pos := binary.BigEndian.AppendUint32(nil, uint32(cr.position))
	nonKey := encode.Encode(nil, [][]byte{pos})

	return btree.NewRecord(nil, key, nonKey)
}

func DecodeColumnMetaRecord(record btree.Record) (ColumnMetaRecord, error) {
	// key = [fileId, name]
	key, err := encode.Decode(record.Key())
	if err != nil {
		return ColumnMetaRecord{}, err
	}
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	name := string(key[1])

	// nonKey = [pos]
	nonKey, err := encode.Decode(record.NonKey())
	if err != nil {
		return ColumnMetaRecord{}, err
	}
	pos := int(binary.BigEndian.Uint32(nonKey[0]))

	return NewColumnMetaRecord(fileId, name, pos), nil
}

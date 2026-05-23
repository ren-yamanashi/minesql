package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ColumnRecord struct {
	fileId   page.FileId // カラムが属するテーブルの FileId
	name     string      // カラム名
	position int         // テーブル上のカラム位置
}

func NewColumnRecord(fileId page.FileId, name string, pos int) ColumnRecord {
	return ColumnRecord{
		fileId:   fileId,
		name:     name,
		position: pos,
	}
}

func (cr ColumnRecord) FileId() page.FileId { return cr.fileId }
func (cr ColumnRecord) Name() string        { return cr.name }
func (cr ColumnRecord) Position() int       { return cr.position }

// encode は btree.Record にエンコードする
func (cr ColumnRecord) encode() btree.Record {
	// key = fileId + name
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(cr.fileId))
	encode.Encode([][]byte{fileId, []byte(cr.name)}, &key)

	// nonKey = pos
	var nonKey []byte
	pos := binary.BigEndian.AppendUint32(nil, uint32(cr.position))
	encode.Encode([][]byte{pos}, &nonKey)

	return btree.NewRecord(nil, key, nonKey)
}

// decodeColumnRecord は btree.Record から columnRecord にデコードする
func decodeColumnRecord(record btree.Record) ColumnRecord {
	// key = [fileId, name]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	name := string(key[1])

	// nonKey = [pos]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	pos := int(binary.BigEndian.Uint32(nonKey[0]))

	return NewColumnRecord(fileId, name, pos)
}

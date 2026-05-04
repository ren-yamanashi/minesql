package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ColumnRecord struct {
	FileId page.FileId // カラムが属するテーブルの FileId
	Name   string      // カラム名
	Pos    int         // テーブル上のカラム位置
}

func NewColumnRecord(fileId page.FileId, name string, pos int) ColumnRecord {
	return ColumnRecord{
		FileId: fileId,
		Name:   name,
		Pos:    pos,
	}
}

// encode は node.Record にエンコードする
func (cr ColumnRecord) encode() node.Record {
	// key = fileId + name
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(cr.FileId))
	encode.Encode([][]byte{fileId, []byte(cr.Name)}, &key)

	// nonKey = pos
	var nonKey []byte
	pos := binary.BigEndian.AppendUint32(nil, uint32(cr.Pos))
	encode.Encode([][]byte{pos}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeColumnRecord は node.Record から columnRecord にデコードする
func decodeColumnRecord(record node.Record) ColumnRecord {
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

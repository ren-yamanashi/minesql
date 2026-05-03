package dictionary

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type columnRecord struct {
	fileId page.FileId // カラムが属するテーブルの FileId
	name   string      // カラム名
	pos    int         // テーブル上のカラム位置
}

func newColumnRecord(fileId page.FileId, name string, pos int) columnRecord {
	return columnRecord{
		fileId: fileId,
		name:   name,
		pos:    pos,
	}
}

// encode は node.Record にエンコードする
func (cr columnRecord) encode() node.Record {
	// key = fileId + name
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(cr.fileId))
	encode.Encode([][]byte{fileId, []byte(cr.name)}, &key)

	// nonKey = pos
	var nonKey []byte
	pos := binary.BigEndian.AppendUint32(nil, uint32(cr.pos))
	encode.Encode([][]byte{pos}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeColumnRecord は node.Record から columnRecord にデコードする
func decodeColumnRecord(record node.Record) columnRecord {
	// key = [fileId, name]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	name := string(key[1])

	// nonKey = [pos]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	pos := int(binary.BigEndian.Uint32(nonKey[0]))

	return newColumnRecord(fileId, name, pos)
}

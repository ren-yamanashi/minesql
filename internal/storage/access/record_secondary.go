package access

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

// SecondaryRecord はセカンダリインデックスレコード
type SecondaryRecord struct {
	skCount    int
	deleteMark byte
	data       [][]byte
}

func newSecondaryRecord(skCount int, deleteMark byte, data [][]byte) *SecondaryRecord {
	return &SecondaryRecord{
		skCount:    skCount,
		deleteMark: deleteMark,
		data:       data,
	}
}

// encode は node.Record にエンコードする
func (sr *SecondaryRecord) encode() node.Record {
	var key []byte
	encode.Encode(sr.data[:sr.skCount], &key)
	skByteLen := len(key)
	encode.Encode(sr.data[sr.skCount:], &key)

	nonKey := make([]byte, 2)
	binary.BigEndian.PutUint16(nonKey, uint16(skByteLen))

	return node.NewRecord([]byte{sr.deleteMark}, key, nonKey)
}

// encodedSecondaryKey はエンコード済みのセカンダリキーを返す
//
// Btree 上のキー (SK + PK) ではなく SK のみ
func (sr *SecondaryRecord) encodedSecondaryKey() []byte {
	var sk []byte
	encode.Encode(sr.data[:sr.skCount], &sk)
	return sk
}

package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

// PrimaryRecord はプライマリインデックスレコード
type PrimaryRecord struct {
	pkCount    int
	deleteMark byte
	data       [][]byte
}

func newPrimaryRecord(pkCount int, deleteMark byte, data [][]byte) *PrimaryRecord {
	return &PrimaryRecord{
		pkCount:    pkCount,
		deleteMark: deleteMark,
		data:       data,
	}
}

// encode は node.Record にエンコードする
func (pr *PrimaryRecord) encode() node.Record {
	var key []byte
	var nonKey []byte
	encode.Encode(pr.data[:pr.pkCount], &key)
	encode.Encode(pr.data[pr.pkCount:], &nonKey)
	return node.NewRecord([]byte{pr.deleteMark}, key, nonKey)
}

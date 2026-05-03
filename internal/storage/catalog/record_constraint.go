package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type constraintRecord struct {
	fileId         page.FileId // 制約が属するテーブルの FileId
	colName        string      // 制約のあるカラム名
	constraintName string      // 制約名
	refTableFileId page.FileId // 制約により参照されるテーブルの FileId
	refColName     string      // 制約により参照されるカラム名
}

func newConstraintRecord(
	fileId page.FileId,
	colName string,
	constraintName string,
	refTableFileId page.FileId,
	refColName string,
) constraintRecord {
	return constraintRecord{
		fileId:         fileId,
		colName:        colName,
		constraintName: constraintName,
		refTableFileId: refTableFileId,
		refColName:     refColName,
	}
}

// encode は node.Record にエンコードする
func (cr constraintRecord) encode() node.Record {
	// key = fileId + colName + constraintName
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(cr.fileId))
	encode.Encode([][]byte{fileId, []byte(cr.colName), []byte(cr.constraintName)}, &key)

	// nonKey = refTableFileId + refColName
	var nonKey []byte
	refTableFileId := binary.BigEndian.AppendUint32(nil, uint32(cr.refTableFileId))
	encode.Encode([][]byte{refTableFileId, []byte(cr.refColName)}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeConstraintRecord は node.Record から constraintRecord にデコードする
func decodeConstraintRecord(record node.Record) constraintRecord {
	// key = [fileId, colName, constraintName]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	colName := string(key[1])
	constraintName := string(key[2])

	// nonKey = [refTableFileId, refColName]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	refTableFileId := page.FileId(binary.BigEndian.Uint32(nonKey[0]))
	refColName := string(nonKey[1])

	return newConstraintRecord(fileId, colName, constraintName, refTableFileId, refColName)
}

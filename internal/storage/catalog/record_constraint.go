package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ConstraintRecord struct {
	FileId         page.FileId // 制約が属するテーブルの FileId
	ColName        string      // 制約のあるカラム名
	ConstraintName string      // 制約名
	RefTableFileId page.FileId // 制約により参照されるテーブルの FileId
	RefColName     string      // 制約により参照されるカラム名
}

func NewConstraintRecord(
	fileId page.FileId,
	colName string,
	constraintName string,
	refTableFileId page.FileId,
	refColName string,
) ConstraintRecord {
	return ConstraintRecord{
		FileId:         fileId,
		ColName:        colName,
		ConstraintName: constraintName,
		RefTableFileId: refTableFileId,
		RefColName:     refColName,
	}
}

// encode は node.Record にエンコードする
func (cr ConstraintRecord) encode() node.Record {
	// key = fileId + colName + constraintName
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(cr.FileId))
	encode.Encode([][]byte{fileId, []byte(cr.ColName), []byte(cr.ConstraintName)}, &key)

	// nonKey = refTableFileId + refColName
	var nonKey []byte
	refTableFileId := binary.BigEndian.AppendUint32(nil, uint32(cr.RefTableFileId))
	encode.Encode([][]byte{refTableFileId, []byte(cr.RefColName)}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeConstraintRecord は node.Record から constraintRecord にデコードする
func decodeConstraintRecord(record node.Record) ConstraintRecord {
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

	return NewConstraintRecord(fileId, colName, constraintName, refTableFileId, refColName)
}

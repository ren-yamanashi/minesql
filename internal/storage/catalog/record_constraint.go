package catalog

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ConstraintRecord struct {
	fileId               page.FileId // 制約が属するテーブルの FileId
	columnName           string      // 制約のあるカラム名
	constraintName       string      // 制約名
	referenceTableFileId page.FileId // 制約により参照されるテーブルの FileId
	referenceColumnName  string      // 制約により参照されるカラム名
}

func NewConstraintRecord(
	fileId page.FileId,
	columnName string,
	constraintName string,
	referenceTableFileId page.FileId,
	referenceColumnName string,
) ConstraintRecord {
	return ConstraintRecord{
		fileId:               fileId,
		columnName:           columnName,
		constraintName:       constraintName,
		referenceTableFileId: referenceTableFileId,
		referenceColumnName:  referenceColumnName,
	}
}

func (cr ConstraintRecord) FileId() page.FileId               { return cr.fileId }
func (cr ConstraintRecord) ColumnName() string                { return cr.columnName }
func (cr ConstraintRecord) ConstraintName() string            { return cr.constraintName }
func (cr ConstraintRecord) ReferenceTableFileId() page.FileId { return cr.referenceTableFileId }
func (cr ConstraintRecord) ReferenceColumnName() string       { return cr.referenceColumnName }

// encode は btree.Record にエンコードする
func (cr ConstraintRecord) encode() btree.Record {
	// key = fileId + colName + constraintName
	var key []byte
	fileId := binary.BigEndian.AppendUint32(nil, uint32(cr.fileId))
	encode.Encode([][]byte{fileId, []byte(cr.columnName), []byte(cr.constraintName)}, &key)

	// nonKey = refTableFileId + refColumnName
	var nonKey []byte
	refTableFileId := binary.BigEndian.AppendUint32(nil, uint32(cr.referenceTableFileId))
	encode.Encode([][]byte{refTableFileId, []byte(cr.referenceColumnName)}, &nonKey)

	return btree.NewRecord(nil, key, nonKey)
}

// decodeConstraintRecord は btree.Record から constraintRecord にデコードする
func decodeConstraintRecord(record btree.Record) ConstraintRecord {
	// key = [fileId, colName, constraintName]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	colName := string(key[1])
	constraintName := string(key[2])

	// nonKey = [refTableFileId, refColumnName]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	refTableFileId := page.FileId(binary.BigEndian.Uint32(nonKey[0]))
	refColumnName := string(nonKey[1])

	return NewConstraintRecord(fileId, colName, constraintName, refTableFileId, refColumnName)
}

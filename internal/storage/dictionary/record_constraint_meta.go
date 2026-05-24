package dictionary

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ConstraintMetaRecord struct {
	fileId               page.FileId // 制約が属するテーブルの FileId
	columnName           string      // 制約のあるカラム名
	constraintName       string      // 制約名
	referenceTableFileId page.FileId // 制約により参照されるテーブルの FileId
	referenceColumnName  string      // 制約により参照されるカラム名
}

func NewConstraintMetaRecord(
	fileId page.FileId,
	columnName string,
	constraintName string,
	referenceTableFileId page.FileId,
	referenceColumnName string,
) ConstraintMetaRecord {
	return ConstraintMetaRecord{
		fileId:               fileId,
		columnName:           columnName,
		constraintName:       constraintName,
		referenceTableFileId: referenceTableFileId,
		referenceColumnName:  referenceColumnName,
	}
}

func (cr ConstraintMetaRecord) FileId() page.FileId               { return cr.fileId }
func (cr ConstraintMetaRecord) ColumnName() string                { return cr.columnName }
func (cr ConstraintMetaRecord) ConstraintName() string            { return cr.constraintName }
func (cr ConstraintMetaRecord) ReferenceTableFileId() page.FileId { return cr.referenceTableFileId }
func (cr ConstraintMetaRecord) ReferenceColumnName() string       { return cr.referenceColumnName }

func (cr ConstraintMetaRecord) Encode() btree.Record {
	// key = fileId + colName + constraintName
	fileId := binary.BigEndian.AppendUint32(nil, uint32(cr.fileId))
	key := encode.Encode(nil, [][]byte{fileId, []byte(cr.columnName), []byte(cr.constraintName)})

	// nonKey = refTableFileId + refColumnName
	refTableFileId := binary.BigEndian.AppendUint32(nil, uint32(cr.referenceTableFileId))
	nonKey := encode.Encode(nil, [][]byte{refTableFileId, []byte(cr.referenceColumnName)})

	return btree.NewRecord(nil, key, nonKey)
}

func DecodeConstraintMetaRecord(record btree.Record) (ConstraintMetaRecord, error) {
	// key = [fileId, colName, constraintName]
	key, err := encode.Decode(record.Key())
	if err != nil {
		return ConstraintMetaRecord{}, err
	}
	fileId := page.FileId(binary.BigEndian.Uint32(key[0]))
	colName := string(key[1])
	constraintName := string(key[2])

	// nonKey = [refTableFileId, refColumnName]
	nonKey, err := encode.Decode(record.NonKey())
	if err != nil {
		return ConstraintMetaRecord{}, err
	}
	refTableFileId := page.FileId(binary.BigEndian.Uint32(nonKey[0]))
	refColumnName := string(nonKey[1])

	return NewConstraintMetaRecord(fileId, colName, constraintName, refTableFileId, refColumnName), nil
}

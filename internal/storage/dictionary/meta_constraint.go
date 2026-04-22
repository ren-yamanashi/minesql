package dictionary

import (
	"encoding/binary"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/page"
)

// ConstraintType は制約の種類
type ConstraintType string

const (
	ConstraintTypePrimaryKey ConstraintType = "PRIMARY"
	ConstraintTypeUniqueKey  ConstraintType = "UNIQUE"
	ConstraintTypeForeignKey ConstraintType = "FOREIGN KEY"
)

// ConstraintMeta はカラムの制約情報を表すメタデータ
type ConstraintMeta struct {
	MetaPageId     page.PageId // 制約メタデータが格納される B+Tree のメタページ ID
	FileId         page.FileId // 制約が属するテーブルの FileId
	ColName        string      // カラム名
	ConstraintName string      // 制約名 (主キーの場合は "PRIMARY")
	RefTableName   string      // 参照先テーブル名 (主キー/ユニークキーの場合は空文字)
	RefColName     string      // 参照先カラム名 (主キー/ユニークキーの場合は空文字)
}

func NewConstraintMeta(fileId page.FileId, colName string, constraintName string, refTableName string, refColName string) *ConstraintMeta {
	return &ConstraintMeta{
		FileId:         fileId,
		ColName:        colName,
		ConstraintName: constraintName,
		RefTableName:   refTableName,
		RefColName:     refColName,
	}
}

// Insert は制約メタデータを B+Tree に挿入する
func (cm *ConstraintMeta) Insert(bp *buffer.BufferPool) error {
	btr := btree.NewBTree(cm.MetaPageId)

	// キーフィールドをエンコード (FileId + ColName + ConstraintName)
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint32(nil, uint32(cm.FileId))
	encode.Encode([][]byte{keyBuf, []byte(cm.ColName), []byte(cm.ConstraintName)}, &encodedKey)

	// 非キーフィールドをエンコード
	// 参照先の有無を示すフラグ (1 バイト) を先頭に配置する
	// memcomparable は空のバイト列をエンコードできないため、参照先がない場合 (PK/UK) はフラグのみ
	var encodedNonKey []byte
	if cm.RefTableName != "" {
		encode.Encode([][]byte{{1}, []byte(cm.RefTableName), []byte(cm.RefColName)}, &encodedNonKey)
	} else {
		encode.Encode([][]byte{{0}}, &encodedNonKey)
	}

	// B+Tree に挿入
	return btr.Insert(bp, node.NewRecord(nil, encodedKey, encodedNonKey))
}

// loadConstraintMeta は指定されたテーブルの制約メタデータを読み込む
//
// 制約メタデータの B+Tree を走査して、指定されたテーブルの制約を収集する
//
//   - bp: BufferPool
//   - fileId: 制約メタデータを読み込む対象のテーブルの FileId
//   - metaPageId: 制約メタデータが格納されている B+Tree のメタページ ID
func loadConstraintMeta(bp *buffer.BufferPool, fileId page.FileId, metaPageId page.PageId) ([]*ConstraintMeta, error) {
	// B+Tree を開く
	conMetaTree := btree.NewBTree(metaPageId)
	iter, err := conMetaTree.Search(bp, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var constraints []*ConstraintMeta
	for {
		record, ok := iter.Get()
		if !ok {
			break
		}

		// キーフィールドをデコード (FileId, ColName, ConstraintName)
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		conFileId := page.FileId(binary.BigEndian.Uint32(keyParts[0]))

		// 指定されたテーブルの制約のみを収集
		if conFileId == fileId {
			colName := string(keyParts[1])
			constraintName := string(keyParts[2])

			// 非キーフィールドをデコード (フラグ, RefTableName, RefColName)
			var nonKeyParts [][]byte
			encode.Decode(record.NonKeyBytes(), &nonKeyParts)
			var refTableName, refColName string
			if nonKeyParts[0][0] == 1 {
				refTableName = string(nonKeyParts[1])
				refColName = string(nonKeyParts[2])
			}

			constraints = append(constraints, &ConstraintMeta{
				FileId:         fileId,
				ColName:        colName,
				ConstraintName: constraintName,
				RefTableName:   refTableName,
				RefColName:     refColName,
			})
		}

		if err := iter.Advance(bp); err != nil {
			return nil, err
		}
	}

	return constraints, nil
}

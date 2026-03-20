package catalog

import (
	"encoding/binary"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/memcomparable"
	"minesql/internal/storage/page"
	"sort"
)

type ColumnType string

const (
	ColumnTypeString ColumnType = "string"
)

// ColumnMetadata はカラムのメタデータを表す
//
// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-columns-table.html
type ColumnMetadata struct {
	MetaPageId page.PageId // カラムのメタデータが格納される B+Tree のメタページID
	FileId     page.FileId // カラムが属するテーブルの FileId
	Name       string      // カラムの名前
	Pos        uint16      // 0 から始まり連続的に増加する、テーブル内のカラムの順序位置
	Type       ColumnType  // カラムのデータ型
}

func NewColumnMetadata(fileId page.FileId, name string, pos uint16, columnType ColumnType) *ColumnMetadata {
	return &ColumnMetadata{
		FileId: fileId,
		Name:   name,
		Pos:    pos,
		Type:   columnType,
	}
}

// Insert はカラムメタデータを B+Tree に挿入する
func (cm *ColumnMetadata) Insert(bp *bufferpool.BufferPool) error {
	btr := btree.NewBPlusTree(cm.MetaPageId)

	// key (FileId + ColName) をエンコード
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint32(nil, uint32(cm.FileId))
	memcomparable.Encode([][]byte{keyBuf, []byte(cm.Name)}, &encodedKey)

	// value (Pos, Type) をエンコード
	var encodedValue []byte
	posBuf := binary.BigEndian.AppendUint16(nil, cm.Pos)
	memcomparable.Encode([][]byte{posBuf, []byte(cm.Type)}, &encodedValue)

	// B+Tree に挿入
	return btr.Insert(bp, btree.NewPair(encodedKey, encodedValue))
}

// loadColumnMetadata は指定されたテーブルのカラムメタデータを読み込む
//
// カラムメタデータの B+Tree を走査して、指定されたテーブルのカラムを収集する
//
// bp: BufferPool
//
// fileId: カラムメタデータを読み込む対象のテーブルの FileId
//
// metaPageId: カラムメタデータが格納されている B+Tree のメタページID
func loadColumnMetadata(bp *bufferpool.BufferPool, fileId page.FileId, metaPageId page.PageId) ([]*ColumnMetadata, error) {
	// B+Tree を開く
	colMetaTree := btree.NewBPlusTree(metaPageId)
	iter, err := colMetaTree.Search(bp, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var cols []*ColumnMetadata
	for {
		pair, ok := iter.Get()
		if !ok {
			break
		}

		// キーをデコード (FileId, ColName)
		var keyParts [][]byte
		memcomparable.Decode(pair.Key, &keyParts)
		colFileId := page.FileId(binary.BigEndian.Uint32(keyParts[0]))

		// 指定されたテーブルのカラムのみを収集
		if colFileId == fileId {
			colName := string(keyParts[1])

			// 値をデコード (Pos, Type)
			var valueParts [][]byte
			memcomparable.Decode(pair.Value, &valueParts)
			pos := binary.BigEndian.Uint16(valueParts[0])
			colType := ColumnType(string(valueParts[1]))

			cols = append(cols, &ColumnMetadata{
				FileId: fileId,
				Name:   colName,
				Pos:    pos,
				Type:   colType,
			})
		}

		if err := iter.Advance(bp); err != nil {
			return nil, err
		}
	}

	// Pos でソート (B+Tree はカラム名でソートされているため)
	sort.Slice(cols, func(i, j int) bool {
		return cols[i].Pos < cols[j].Pos
	})

	return cols, nil
}

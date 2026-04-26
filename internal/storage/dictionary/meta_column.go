package dictionary

import (
	"encoding/binary"
	"sort"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type ColumnType string

const (
	ColumnTypeString ColumnType = "string"
)

// ColumnMeta はカラムのメタデータを表す
type ColumnMeta struct {
	MetaPageId page.PageId // カラムのメタデータが格納される B+Tree のメタページID
	FileId     page.FileId // カラムが属するテーブルの FileId
	Name       string      // カラム名
	Pos        uint16      // 0 から始まり連続的に増加する、テーブル内のカラムの順序位置
	Type       ColumnType  // カラムのデータ型
}

func NewColumnMeta(fileId page.FileId, name string, pos uint16, columnType ColumnType) *ColumnMeta {
	return &ColumnMeta{
		FileId: fileId,
		Name:   name,
		Pos:    pos,
		Type:   columnType,
	}
}

// Insert はカラムメタデータを B+Tree に挿入する
func (cm *ColumnMeta) Insert(bp *buffer.BufferPool) error {
	btr := btree.NewBTree(cm.MetaPageId)

	// キーフィールドをエンコード (FileId + ColName)
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint32(nil, uint32(cm.FileId))
	encode.Encode([][]byte{keyBuf, []byte(cm.Name)}, &encodedKey)

	// 非キーフィールドをエンコード (Pos, Type)
	var encodedNonKey []byte
	posBuf := binary.BigEndian.AppendUint16(nil, cm.Pos)
	encode.Encode([][]byte{posBuf, []byte(cm.Type)}, &encodedNonKey)

	// B+Tree に挿入
	return btr.Insert(bp, node.NewRecord(nil, encodedKey, encodedNonKey))
}

// loadColumnMeta は指定されたテーブルのカラムメタデータを読み込む
//
// カラムメタデータの B+Tree を走査して、指定されたテーブルのカラムを収集する
//
//   - bp: BufferPool
//   - fileId: カラムメタデータを読み込む対象のテーブルの FileId
//   - metaPageId: カラムメタデータが格納されている B+Tree のメタページID
func loadColumnMeta(bp *buffer.BufferPool, fileId page.FileId, metaPageId page.PageId) ([]*ColumnMeta, error) {
	// B+Tree を開く
	colMetaTree := btree.NewBTree(metaPageId)
	iter, err := colMetaTree.Search(bp, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var cols []*ColumnMeta
	for {
		record, ok := iter.Get()
		if !ok {
			break
		}

		// キーフィールドをデコード (FileId, ColName)
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		colFileId := page.FileId(binary.BigEndian.Uint32(keyParts[0]))

		// 指定されたテーブルのカラムのみを収集
		if colFileId == fileId {
			colName := string(keyParts[1])

			// 非キーフィールドをデコード (Pos, Type)
			var nonKeyParts [][]byte
			encode.Decode(record.NonKeyBytes(), &nonKeyParts)
			pos := binary.BigEndian.Uint16(nonKeyParts[0])
			colType := ColumnType(string(nonKeyParts[1]))

			cols = append(cols, &ColumnMeta{
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

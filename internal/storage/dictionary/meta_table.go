package dictionary

import (
	"encoding/binary"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/page"
)

// TableMeta はテーブルのメタデータを表す
type TableMeta struct {
	MetaPageId     page.PageId   // テーブルのメタデータが格納される B+Tree のメタページID
	FileId         page.FileId   // テーブルの実データが格納されるディスクファイルの識別子
	Name           string        // テーブル名
	NCols          uint8         // カラム数
	PKCount        uint8         // プライマリキーのカラム数 (プライマリキーは先頭から連続している想定) (例: PK が (id, name) の場合、PKCount は 2)
	DataMetaPageId page.PageId   // 実データが格納される B+Tree のメタページID
	Cols           []*ColumnMeta // テーブルのカラム情報
	Indexes        []*IndexMeta  // テーブルのインデックス情報
}

func NewTableMeta(fileId page.FileId, name string, nCols uint8, pkCount uint8, cols []*ColumnMeta, indexes []*IndexMeta, dataMetaPageId page.PageId) TableMeta {
	return TableMeta{
		FileId:         fileId,
		Name:           name,
		NCols:          nCols,
		PKCount:        pkCount,
		Cols:           cols,
		Indexes:        indexes,
		DataMetaPageId: dataMetaPageId,
	}
}

// GetSortedCols はカラムの位置 (Pos) でソートされたカラムメタデータを取得する
func (tm *TableMeta) GetSortedCols() []*ColumnMeta {
	sortedColMeta := make([]*ColumnMeta, len(tm.Cols))
	for _, colMeta := range tm.Cols {
		sortedColMeta[colMeta.Pos] = colMeta
	}
	return sortedColMeta
}

// GetColByName はカラム名からカラムを取得する
func (tm *TableMeta) GetColByName(colName string) (*ColumnMeta, bool) {
	for _, col := range tm.Cols {
		if col.Name == colName {
			return col, true
		}
	}
	return nil, false
}

// GetIndexByColName は指定されたカラム名で構成されるインデックスを取得する
func (tm *TableMeta) GetIndexByColName(colName string) (*IndexMeta, bool) {
	for _, idx := range tm.Indexes {
		if idx.ColName == colName {
			return idx, true
		}
	}
	return nil, false
}

// Insert はテーブルメタデータを B+Tree に挿入する
func (tm *TableMeta) Insert(bp *buffer.BufferPool) error {
	btr := btree.NewBTree(tm.MetaPageId)

	// キーフィールドをエンコード (FileId)
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint32(nil, uint32(tm.FileId))
	encode.Encode([][]byte{keyBuf}, &encodedKey)

	// 非キーフィールドをエンコード (Name, NCols, PKCount, DataMetaPageId)
	var encodedNonKey []byte
	nColsBuf := binary.BigEndian.AppendUint64(nil, uint64(tm.NCols))
	pkCountBuf := binary.BigEndian.AppendUint64(nil, uint64(tm.PKCount))
	encode.Encode([][]byte{[]byte(tm.Name), nColsBuf, pkCountBuf, tm.DataMetaPageId.ToBytes()}, &encodedNonKey)

	// B+Tree に挿入
	return btr.Insert(bp, node.NewRecord(nil, encodedKey, encodedNonKey))
}

// loadTableMeta は指定されたテーブルのメタデータを読み込む
//
// テーブルメタデータの B+Tree を走査して、指定されたテーブルのメタデータを読み込む
//
//   - bp: BufferPool
//   - tableMetaPageId: テーブルメタデータが格納されている B+Tree のメタページID
//   - indexMetaPageId: インデックスメタデータが格納されている B+Tree のメタページID
//   - columnMetaPageId: カラムメタデータが格納されている B+Tree のメタページID
func loadTableMeta(bp *buffer.BufferPool, tableMetaPageId page.PageId, indexMetaPageId page.PageId, columnMetaPageId page.PageId) ([]*TableMeta, error) {
	// B+Tree を開く
	tableMetaTree := btree.NewBTree(tableMetaPageId)
	iter, err := tableMetaTree.Search(bp, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var tables []*TableMeta
	for {
		record, ok := iter.Get()
		if !ok {
			break
		}

		// キーフィールドをデコード (FileId)
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		fileId := page.FileId(binary.BigEndian.Uint32(keyParts[0]))

		// 非キーフィールドをデコード (Name, NCols, PKCount, DataMetaPageId)
		var nonKeyParts [][]byte
		encode.Decode(record.NonKeyBytes(), &nonKeyParts)
		name := string(nonKeyParts[0])
		nCols := uint8(binary.BigEndian.Uint64(nonKeyParts[1]))
		pkCount := uint8(binary.BigEndian.Uint64(nonKeyParts[2]))
		dataMetaPageId := page.RestorePageIdFromBytes(nonKeyParts[3])

		// インデックスメタデータを読み込む
		indexes, err := loadIndexMeta(bp, fileId, indexMetaPageId)
		if err != nil {
			return nil, err
		}

		// カラムメタデータを読み込む
		cols, err := loadColumnMeta(bp, fileId, columnMetaPageId)
		if err != nil {
			return nil, err
		}

		tables = append(tables, &TableMeta{
			FileId:         fileId,
			Name:           name,
			NCols:          nCols,
			PKCount:        pkCount,
			DataMetaPageId: dataMetaPageId,
			Indexes:        indexes,
			Cols:           cols,
		})

		if err := iter.Advance(bp); err != nil {
			return nil, err
		}
	}

	return tables, nil
}

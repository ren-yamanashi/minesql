package catalog

import (
	"encoding/binary"
	"fmt"
	"minesql/internal/access"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/memcomparable"
	"minesql/internal/storage/page"
)

// TableMetadata はテーブルのメタデータを表す
//
// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-tables-access.html
type TableMetadata struct {
	MetaPageId      page.PageId       // テーブルのメタデータが格納される B+Tree のメタページID
	FileId          page.FileId       // テーブルの識別子 (一意)
	Name            string            // テーブルの名前
	NCols           uint8             // テーブルの列数
	PrimaryKeyCount uint8             // プライマリキーの列数 (プライマリキーは先頭から連続している想定) (例: プライマリキーが (id, name) の場合、PrimaryKeyCount は 2 になる)
	DataMetaPageId  page.PageId       // 実データが格納される B+Tree のメタページID
	Cols            []*ColumnMetadata // テーブルのカラム情報
	Indexes         []*IndexMetadata  // テーブルのインデックス情報
}

func NewTableMetadata(fileId page.FileId, name string, nCols uint8, pkCount uint8, cols []*ColumnMetadata, indexes []*IndexMetadata, dataMetaPageId page.PageId) TableMetadata {
	return TableMetadata{
		FileId:          fileId,
		Name:            name,
		NCols:           nCols,
		PrimaryKeyCount: pkCount,
		Cols:            cols,
		Indexes:         indexes,
		DataMetaPageId:  dataMetaPageId,
	}
}

// GetSortedCols はカラムの位置 (Pos) でソートされたカラムメタデータを取得する
func (tm *TableMetadata) GetSortedCols() []*ColumnMetadata {
	sortedColMeta := make([]*ColumnMetadata, len(tm.Cols))
	for _, colMeta := range tm.Cols {
		sortedColMeta[colMeta.Pos] = colMeta
	}
	return sortedColMeta
}

// GetColByName はカラム名からカラムを取得する
func (tm *TableMetadata) GetColByName(colName string) (*ColumnMetadata, bool) {
	for _, col := range tm.Cols {
		if col.Name == colName {
			return col, true
		}
	}
	return nil, false
}

// GetIndexByColName は指定されたカラム名で構成されるインデックスを取得する
func (tm *TableMetadata) GetIndexByColName(colName string) (*IndexMetadata, bool) {
	for _, idx := range tm.Indexes {
		if idx.ColName == colName {
			return idx, true
		}
	}
	return nil, false
}

// GetTable はテーブル (access.Table) を取得する
func (tm *TableMetadata) GetTable() (*access.TableAccessMethod, error) {
	// ユニークインデックスを構築
	var uniqueIndexes []*access.UniqueIndexAccessMethod
	for _, idxMeta := range tm.Indexes {
		if idxMeta.Type == IndexTypeUnique {
			colMeta, ok := tm.GetColByName(idxMeta.ColName)
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", idxMeta.ColName, tm.Name)
			}
			ui := access.NewUniqueIndexAccessMethod(idxMeta.Name, idxMeta.ColName, idxMeta.DataMetaPageId, colMeta.Pos)
			uniqueIndexes = append(uniqueIndexes, ui)
		}
	}

	// テーブルを構築
	tbl := access.NewTableAccessMethod(tm.Name, tm.DataMetaPageId, tm.PrimaryKeyCount, uniqueIndexes)
	return &tbl, nil
}

// Insert はテーブルメタデータを B+Tree に挿入する
func (tm *TableMetadata) Insert(bp *bufferpool.BufferPool) error {
	btr := btree.NewBPlusTree(tm.MetaPageId)

	// key (FileId) をエンコード
	var encodedKey []byte
	keyBuf := binary.BigEndian.AppendUint32(nil, uint32(tm.FileId))
	memcomparable.Encode([][]byte{keyBuf}, &encodedKey)

	// value (Name, NCols, PrimaryKeyCount, DataMetaPageId) をエンコード
	var encodedValue []byte
	nColsBuf := binary.BigEndian.AppendUint64(nil, uint64(tm.NCols))
	pkCountBuf := binary.BigEndian.AppendUint64(nil, uint64(tm.PrimaryKeyCount))
	memcomparable.Encode([][]byte{[]byte(tm.Name), nColsBuf, pkCountBuf, tm.DataMetaPageId.ToBytes()}, &encodedValue)

	// B+Tree に挿入
	return btr.Insert(bp, btree.NewRecord(nil, encodedKey, encodedValue))
}

// loadTableMetadata は指定されたテーブルのメタデータを読み込む
//
// テーブルメタデータの B+Tree を走査して、指定されたテーブルのメタデータを読み込む
//
// bp: BufferPool
//
// tableMetaPageId: テーブルメタデータが格納されている B+Tree のメタページID
//
// indexMetaPageId: インデックスメタデータが格納されている B+Tree のメタページID
//
// columnMetaPageId: カラムメタデータが格納されている B+Tree のメタページID
func loadTableMetadata(bp *bufferpool.BufferPool, tableMetaPageId page.PageId, indexMetaPageId page.PageId, columnMetaPageId page.PageId) ([]*TableMetadata, error) {
	// B+Tree を開く
	tableMetaTree := btree.NewBPlusTree(tableMetaPageId)
	iter, err := tableMetaTree.Search(bp, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var tables []*TableMetadata

	for {
		record, ok := iter.Get()
		if !ok {
			break
		}

		// キーをデコード (FileId)
		var keyParts [][]byte
		memcomparable.Decode(record.KeyBytes(), &keyParts)
		fileId := page.FileId(binary.BigEndian.Uint32(keyParts[0]))

		// 値をデコード (Name, NCols, PrimaryKeyCount, DataMetaPageId)
		var valueParts [][]byte
		memcomparable.Decode(record.NonKeyBytes(), &valueParts)
		name := string(valueParts[0])
		nCols := uint8(binary.BigEndian.Uint64(valueParts[1]))
		pkCount := uint8(binary.BigEndian.Uint64(valueParts[2]))
		dataMetaPageId := page.RestorePageIdFromBytes(valueParts[3])

		// インデックスメタデータを読み込む
		indexes, err := loadIndexMetadata(bp, fileId, indexMetaPageId)
		if err != nil {
			return nil, err
		}

		// カラムメタデータを読み込む
		cols, err := loadColumnMetadata(bp, fileId, columnMetaPageId)
		if err != nil {
			return nil, err
		}

		tables = append(tables, &TableMetadata{
			FileId:          fileId,
			Name:            name,
			NCols:           nCols,
			PrimaryKeyCount: pkCount,
			DataMetaPageId:  dataMetaPageId,
			Indexes:         indexes,
			Cols:            cols,
		})

		if err := iter.Advance(bp); err != nil {
			return nil, err
		}
	}

	return tables, nil
}

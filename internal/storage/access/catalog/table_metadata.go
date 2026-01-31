package catalog

import (
	"fmt"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/page"
)

// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-tables-table.html
type TableMetadata struct {
	// テーブルのメタデータが格納される B+Tree のメタページID
	MetaPageId page.PageId
	// テーブルの識別子 (一意)
	TableId uint64
	// テーブルの名前
	Name string
	// テーブルの列数
	NCols uint8
	// プライマリキーの列数 (プライマリキーは先頭から連続している想定)
	// 例: プライマリキーが (id, name) の場合、PrimaryKeyCount は 2 になる
	PrimaryKeyCount uint8
	// 実データが格納される B+Tree のメタページID
	DataMetaPageId page.PageId
	// テーブルのカラム情報
	Cols []*ColumnMetadata
	// テーブルのインデックス情報
	Indexes []*IndexMetadata
}

func NewTableMetadata(tableId uint64, name string, nCols uint8, pkCount uint8, cols []*ColumnMetadata, indexes []*IndexMetadata, dataMetaPageId page.PageId) TableMetadata {
	return TableMetadata{
		TableId:         tableId,
		Name:            name,
		NCols:           nCols,
		PrimaryKeyCount: pkCount,
		Cols:            cols,
		Indexes:         indexes,
		DataMetaPageId:  dataMetaPageId,
	}
}

// カラム名からカラムを取得
func (tm *TableMetadata) GetColByName(colName string) (*ColumnMetadata, bool) {
	for _, col := range tm.Cols {
		if col.Name == colName {
			return col, true
		}
	}
	return nil, false
}

// 指定されたカラム名で構成されるインデックスを取得
func (tm *TableMetadata) GetIndexByColName(colName string) (*IndexMetadata, bool) {
	for _, idx := range tm.Indexes {
		if idx.ColName == colName {
			return idx, true
		}
	}
	return nil, false
}

// テーブル (table.Table) を取得する
func (tm *TableMetadata) GetTable() (*table.Table, error) {
	// ユニークインデックスを構築
	var uniqueIndexes []*table.UniqueIndex
	for _, idxMeta := range tm.Indexes {
		if idxMeta.Type == IndexTypeUnique {
			colMeta, ok := tm.GetColByName(idxMeta.ColName)
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", idxMeta.ColName, tm.Name)
			}
			ui := table.NewUniqueIndex(idxMeta.Name, idxMeta.ColName, colMeta.Pos)
			ui.MetaPageId = idxMeta.DataMetaPageId
			uniqueIndexes = append(uniqueIndexes, ui)
		}
	}

	// テーブルを構築
	tbl := table.NewTable(tm.Name, tm.DataMetaPageId, tm.PrimaryKeyCount, uniqueIndexes)
	return &tbl, nil
}

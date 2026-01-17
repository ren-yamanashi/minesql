package catalog

import (
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
	// 実データが格納される B+Tree のメタページID
	DataMetaPageId page.PageId
	// テーブルのカラム情報
	Cols []ColumnMetadata
	// テーブルのインデックス情報
	Indexes []IndexMetadata
}

func NewTableMetadata(tableId uint64, name string, nCols uint8, cols []ColumnMetadata, indexes []IndexMetadata, dataMetaPageId page.PageId) TableMetadata {
	return TableMetadata{
		TableId: tableId,
		Name:    name,
		NCols:   nCols,
		Cols:    cols,
		Indexes: indexes,
		DataMetaPageId: dataMetaPageId,
	}
}

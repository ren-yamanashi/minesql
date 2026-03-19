package catalog

import "minesql/internal/storage/page"

type ColumnType string

const (
	ColumnTypeString ColumnType = "string"
)

// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-columns-table.html
type ColumnMetadata struct {
	// カラムのメタデータが格納される B+Tree のメタページID
	MetaPageId page.PageId
	// カラムに関連付けられたテーブルを表す識別子
	TableId uint64
	// カラムの名前
	Name string
	// 0 から始まり連続的に増加する、テーブル内のカラムの順序位置
	Pos uint16
	// カラムのデータ型
	Type ColumnType
}

func NewColumnMetadata(tableId uint64, name string, pos uint16, columnType ColumnType) *ColumnMetadata {
	return &ColumnMetadata{
		TableId: tableId,
		Name:    name,
		Pos:     pos,
		Type:    columnType,
	}
}

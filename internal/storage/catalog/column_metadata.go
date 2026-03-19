package catalog

import "minesql/internal/storage/page"

type ColumnType string

const (
	ColumnTypeString ColumnType = "string"
)

// ColumnMetadata はカラムのメタデータを表す
//
// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-columns-table.html
type ColumnMetadata struct {
	MetaPageId page.PageId // カラムのメタデータが格納される B+Tree のメタページID
	TableId    uint64      // カラムに関連付けられたテーブルを表す識別子
	Name       string      // カラムの名前
	Pos        uint16      // 0 から始まり連続的に増加する、テーブル内のカラムの順序位置
	Type       ColumnType  // カラムのデータ型
}

func NewColumnMetadata(tableId uint64, name string, pos uint16, columnType ColumnType) *ColumnMetadata {
	return &ColumnMetadata{
		TableId: tableId,
		Name:    name,
		Pos:     pos,
		Type:    columnType,
	}
}

package catalog

import "minesql/internal/storage/page"

type IndexType string

const (
	IndexTypeUnique IndexType = "unique secondary"
)

// IndexMetadata はセカンダリインデックスのメタデータを表す
//
// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-indexes-table.html
type IndexMetadata struct {
	MetaPageId     page.PageId // インデックスのメタデータが格納される B+Tree のメタページID
	TableId        uint64      // インデックスが関連付けられたテーブルの識別子
	Name           string      // インデックスの名前
	ColName        string      // インデックスを構成するカラム名
	Type           IndexType   // インデックスの種類
	DataMetaPageId page.PageId // 実データが格納される B+Tree のメタページID
}

func NewIndexMetadata(tableId uint64, name string, colName string, indexType IndexType, dataMetaPageId page.PageId) *IndexMetadata {
	return &IndexMetadata{
		TableId:        tableId,
		Name:           name,
		ColName:        colName,
		Type:           indexType,
		DataMetaPageId: dataMetaPageId,
	}
}

package catalog

import "minesql/internal/storage/page"

type IndexType string

const (
	IndexTypeClustered IndexType = "clustered"
	IndexTypeUnique    IndexType = "unique secondary"
	IndexTypeNonUnique IndexType = "non-unique secondary"
)

// 参考: https://dev.mysql.com/doc/refman/8.0/ja/information-schema-innodb-indexes-table.html
// 現状は IndexId を追加してない
type IndexMetadata struct {
	// インデックスのメタデータが格納される B+Tree のメタページID
	MetaPageId page.PageId
	// インデックスが関連付けられたテーブルの識別子
	TableId uint64
	// インデックスの名前
	Name string
	// インデックスの種類
	Type IndexType
	// 実データが格納される B+Tree のメタページID
	DataMetaPageId page.PageId
}

func NewIndexMetadata(tableId uint64, name string, indexType IndexType, dataMetaPageId page.PageId) IndexMetadata {
	return IndexMetadata{
		TableId:        tableId,
		Name:           name,
		Type:           indexType,
		DataMetaPageId: dataMetaPageId,
	}
}

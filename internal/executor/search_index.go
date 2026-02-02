package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
)

// SearchIndex はセカンダリインデックスを使用してレコードを検索する
// whileCondition はフィルタ条件として機能し、false を返すとそのレコードをスキップする
type SearchIndex struct {
	tableName  string
	indexName  string
	searchMode RecordSearchMode
	// フィルタ条件を満たすかどうかを判定する関数（セカンダリキーに対して適用）
	// false を返すとそのレコードをスキップして次のレコードを探す
	whileCondition func(record Record) bool
	indexIterator  *btree.Iterator
	tableIterator  *btree.Iterator
	// テーブル本体の B+Tree (プライマリキーで検索するために使用)
	tableBTree *btree.BTree
}

func NewSearchIndex(
	tableName string,
	indexName string,
	searchMode RecordSearchMode,
	whileCondition func(record Record) bool,
) *SearchIndex {
	return &SearchIndex{
		tableName:      tableName,
		indexName:      indexName,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

// 次の Record を取得する
// whileCondition が true のレコードのみを返す（セカンダリキーに対して判定）
// 条件に一致しないレコードはスキップする
// データがない場合は (nil, nil) を返す
func (is *SearchIndex) Next() (Record, error) {
	sm := storage.GetStorageManager()

	// 初回実行時に B+Tree とイテレータを作成
	if is.indexIterator == nil {
		tblMeta, err := sm.Catalog.GetTableMetadataByName(is.tableName)
		if err != nil {
			return nil, err
		}
		tbl, err := tblMeta.GetTable()
		if err != nil {
			return nil, err
		}
		index, err := tbl.GetUniqueIndexByName(is.indexName)
		if err != nil {
			return nil, err
		}

		// インデックスの B+Tree を取得
		indexBTree := btree.NewBTree(index.MetaPageId)

		// テーブル本体の B+Tree を保持
		is.tableBTree = btree.NewBTree(tbl.MetaPageId)

		// インデックス用のイテレータを作成
		indexIter, err := indexBTree.Search(sm.BufferPoolManager, is.searchMode.Encode())
		if err != nil {
			return nil, err
		}
		is.indexIterator = indexIter
	}

	// 条件に一致するレコードが見つかるまでループ
	for {
		// セカンダリインデックスから次のペアを取得
		secondaryIndexPair, ok, err := is.indexIterator.Next(sm.BufferPoolManager)
		if !ok {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		// セカンダリキーをデコード
		var secondaryKey [][]byte
		table.Decode(secondaryIndexPair.Key, &secondaryKey)

		// フィルタ条件をチェック
		if !is.whileCondition(secondaryKey) {
			// 条件に一致しない場合は次のレコードへ
			continue
		}

		// エンコードされたプライマリキーでテーブル本体を検索
		is.tableIterator, err = is.tableBTree.Search(sm.BufferPoolManager, btree.SearchModeKey{Key: secondaryIndexPair.Value})
		if err != nil {
			return nil, err
		}
		tablePair, ok, err := is.tableIterator.Next(sm.BufferPoolManager)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}

		// レコード (プライマリキー + 値) をデコード
		var record [][]byte
		table.Decode(tablePair.Key, &record)
		table.Decode(tablePair.Value, &record)
		return record, nil
	}
}

package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
)

type IndexScan struct {
	tableName  string
	indexName  string
	searchMode RecordSearchMode
	// 継続条件を満たすかどうかを判定する関数
	whileCondition func(record Record) bool
	indexIterator  *btree.Iterator
	tableIterator  *btree.Iterator
	// テーブル本体の B+Tree (プライマリキーで検索するために使用)
	tableBTree *btree.BTree
}

func NewIndexScan(
	tableName string,
	indexName string,
	searchMode RecordSearchMode,
	whileCondition func(record Record) bool,
) *IndexScan {
	return &IndexScan{
		tableName:      tableName,
		indexName:      indexName,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

// 次の Record を取得する
// データがない場合、継続条件を満たさない場合は (nil, nil) を返す
func (is *IndexScan) Next() (Record, error) {
	engine := storage.GetStorageManager()
	bpm := engine.GetBufferPoolManager()

	// 初回実行時に B+Tree とイテレータを作成
	if is.indexIterator == nil {
		tbl, err := engine.GetTable(is.tableName)
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
		indexIter, err := indexBTree.Search(bpm, is.searchMode.Encode())
		if err != nil {
			return nil, err
		}
		is.indexIterator = indexIter
	}

	// セカンダリインデックスから次のペアを取得
	secondaryIndexPair, ok, err := is.indexIterator.Next(bpm)
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// セカンダリキーをデコード
	var secondaryKey [][]byte
	table.Decode(secondaryIndexPair.Key, &secondaryKey)

	// 継続条件をチェック
	if !is.whileCondition(secondaryKey) {
		return nil, nil
	}

	// エンコードされたプライマリキーでテーブル本体を検索
	is.tableIterator, err = is.tableBTree.Search(bpm, btree.SearchModeKey{Key: secondaryIndexPair.Value})
	if err != nil {
		return nil, err
	}
	tablePair, ok, err := is.tableIterator.Next(bpm)
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

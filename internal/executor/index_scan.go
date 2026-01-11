package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/table"
)

type IndexScan struct {
	tableName  string
	indexName  string
	searchMode RecordSearchMode
	// 継続条件を満たすかどうかを判定する関数
	whileCondition func(record Record) bool
	indexHandler   *storage.TableHandler
	tableHandler   *storage.TableHandler
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
	engine := storage.GetStorageEngine()

	// 初回実行時に handle を作成
	if is.indexHandler == nil {
		indexHandler, err := engine.GetTableHandle(is.tableName)
		if err != nil {
			return nil, err
		}

		// インデックススキャン用の TableHandle を取得
		err = indexHandler.SetIndexIterator(is.indexName, is.searchMode.Encode())
		if err != nil {
			return nil, err
		}
		is.indexHandler = indexHandler

		// テーブル検索用の TableHandle を取得
		tableHandler, err := engine.GetTableHandle(is.tableName)
		if err != nil {
			return nil, err
		}
		is.tableHandler = tableHandler
	}

	// セカンダリインデックスから次のペアを取得
	secondaryIndexPair, ok, err := is.indexHandler.Next()
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

	// エンコードされたプライマリキーでテーブルを検索
	err = is.tableHandler.SetSearchPrimaryKeyIterator(secondaryIndexPair.Value)
	if err != nil {
		return nil, err
	}

	tablePair, ok, err := is.tableHandler.Next()
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

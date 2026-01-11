package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/table"
)

type SequentialScan struct {
	tableName      string
	searchMode     RecordSearchMode
	// 継続条件を満たすかどうかを判定する関数
	whileCondition func(record Record) bool
	tableHandler    *storage.TableHandler
}

func NewSequentialScan(
	tableName string,
	searchMode RecordSearchMode,
	whileCondition func(record Record) bool,
) *SequentialScan {
	return &SequentialScan{
		tableName:      tableName,
		searchMode:     searchMode,
		whileCondition: whileCondition,
	}
}

// 次の Record を取得する
// データがない場合、継続条件を満たさない場合は (nil, nil) を返す
func (ss *SequentialScan) Next() (Record, error) {
	engine := storage.GetStorageEngine()

	// 初回実行時に table handle を作成
	if ss.tableHandler == nil {
		// TableHandle を取得
		handler, err := engine.GetTableHandle(ss.tableName)
		if err != nil {
			return nil, err
		}

		// スキャン用の TableHandle を作成 (RecordSearchMode を btree.SearchMode にエンコード)
		err = handler.SetTableIterator(ss.searchMode.Encode())
		if err != nil {
			return nil, err
		}

		ss.tableHandler = handler
	}

	// レコード取得
	pair, ok, err := ss.tableHandler.Next()
	if !ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// レコード (プライマリキー + 値) をデコード
	var record [][]byte
	table.Decode(pair.Key, &record)
	table.Decode(pair.Value, &record)

	// 継続条件をチェック
	if !ss.whileCondition(record) {
		return nil, nil
	}

	return record, nil
}

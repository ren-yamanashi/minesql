package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
)

type SequentialScan struct {
	// 継続条件を満たすかどうかを判定する関数
	whileCondition func(record Record) bool
	iterator       *btree.Iterator
	tableName      string
	searchMode     RecordSearchMode
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
	sm := storage.GetStorageManager()

	// 初回実行時はイテレータを作成
	if ss.iterator == nil {
		tblMeta, err := sm.Catalog.GetTableMetadataByName(ss.tableName)
		if err != nil {
			return nil, err
		}
		tbl, err := tblMeta.GetTable()
		if err != nil {
			return nil, err
		}

		// テーブルスキャン用のイテレータを作成
		btr := btree.NewBTree(tbl.MetaPageId)
		iterator, err := btr.Search(sm.BufferPoolManager, ss.searchMode.Encode())
		if err != nil {
			return nil, err
		}

		ss.iterator = iterator
	}

	// レコード取得
	pair, ok, err := ss.iterator.Next(sm.BufferPoolManager)
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

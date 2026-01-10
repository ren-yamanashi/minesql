package executor

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
)

type ExecSequentialScan struct {
	// テーブルを走査するイテレータ
	tableIterator *btree.Iterator
	// 継続条件を満たすかどうかを判定する関数
	whileCondition func(record Record) bool
}

func NewExecSequentialScan(
	bpm *bufferpool.BufferPoolManager,
	table table.Table,
	whileCondition func(record Record) bool,
) (*ExecSequentialScan, error) {
	tree := btree.NewBTree(table.MetaPageId)
	iter, err := tree.Search(bpm, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	return &ExecSequentialScan{
		tableIterator:  iter,
		whileCondition: whileCondition,
	}, nil
}

// 次の Record を取得する
// データがない場合、継続条件を満たさない場合は (nil, nil) を返す
func (e *ExecSequentialScan) Next(bpm *bufferpool.BufferPoolManager) (Record, error) {
	pair, ok, err := e.tableIterator.Next(bpm)
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
	if !e.whileCondition(record) {
		return nil, nil
	}

	return record, nil
}

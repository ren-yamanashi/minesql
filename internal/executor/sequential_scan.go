package executor

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
)

type SequentialScan struct {
	// テーブルを走査するイテレータ
	TableIterator *btree.Iterator
	// 継続条件を満たすかどうかを判定する関数
	WhileCondition func(record Record) bool
}

func NewSequentialScan(
	tableIterator *btree.Iterator,
	whileCondition func(record Record) bool,
) *SequentialScan {
	return &SequentialScan{
		TableIterator:  tableIterator,
		WhileCondition: whileCondition,
	}
}

// 次の Record を取得する
// データがない場合、継続条件を満たさない場合は (nil, nil) を返す
func (ss *SequentialScan) Next(bpm *bufferpool.BufferPoolManager) (Record, error) {
	pair, ok, err := ss.TableIterator.Next(bpm)
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
	if !ss.WhileCondition(record) {
		return nil, nil
	}

	return record, nil
}

package main

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
)

// 名前 (2 番目のカラム) が "Charlie" のレコードのみを取得するフィルタースキャン
func filterScan(bpm *bufferpool.BufferPoolManager, tbl table.Table) {
	println("=== フィルタースキャン (名前が 'Charlie' のレコード) ===")

	// フルテーブルスキャン用の継続条件
	whileCondition := func(record executor.Record) bool {
		return true
	}
	btr := btree.NewBTree(tbl.MetaPageId)
	tableIterator, _ := btr.Search(bpm, btree.SearchModeStart{})
	seqScan := executor.NewSequentialScan(
		tableIterator,
		whileCondition,
	)

	// フィルター条件: 名前 (2 番目のカラム) が "Charlie" のレコードのみ
	filterCondition := func(record executor.Record) bool {
		return string(record[1]) == "Charlie"
	}
	filter := executor.NewFilter(seqScan, filterCondition)

	for {
		record, err := filter.Next(bpm)
		if err != nil {
			panic(err)
		}
		if record == nil {
			break
		}
		// レコードの内容を表示
		println(string(record[0]), string(record[1]), string(record[2]))
	}
}

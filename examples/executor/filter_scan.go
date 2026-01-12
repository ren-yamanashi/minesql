package main

import (
	"minesql/internal/executor"
)

// 名前 (2 番目のカラム) が "Charlie" のレコードのみを取得するフィルタースキャン
func filterScan() {
	println("=== フィルタースキャン (名前が 'Charlie' のレコード) ===")

	// フルテーブルスキャン用の継続条件
	whileCondition := func(record executor.Record) bool {
		return true
	}
	seqScan := executor.NewSequentialScan(
		"users",
		executor.RecordSearchModeStart{},
		whileCondition,
	)

	// フィルター条件: 名前 (2 番目のカラム) が "Charlie" のレコードのみ
	filterCondition := func(record executor.Record) bool {
		return string(record[1]) == "Charlie"
	}
	filter := executor.NewFilter(seqScan, filterCondition)
	printRecords(filter)
}

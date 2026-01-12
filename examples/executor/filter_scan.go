package main

import (
	"minesql/internal/executor"
)

// 名前 (2 番目のカラム) が "Charlie" のレコードのみを取得するフィルタースキャン
func filterScan() {
	println("=== フィルタースキャン (名前が 'Charlie' のレコード) ===")

	// フルテーブルスキャン
	seqScan := executor.NewSequentialScan(
		"users",
		executor.RecordSearchModeStart{},
		func(record executor.Record) bool {
			return true
		},
	)

	// フィルター条件: 名前 (2 番目のカラム) が "Charlie" のレコードのみ
	filter := executor.NewFilter(seqScan, func(record executor.Record) bool {
		return string(record[1]) == "Charlie"
	})
	printRecords(filter)
}

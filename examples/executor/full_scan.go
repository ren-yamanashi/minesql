package main

import (
	"minesql/internal/executor"
)

// フルテーブルスキャン
func fullTableScan() {
	println("=== フルテーブルスキャン ===")
	seqScan := executor.NewSequentialScan(
		"users",
		executor.RecordSearchModeStart{},
		func(record executor.Record) bool { // フルテーブルスキャンなので常に true を返す継続条件
			return true
		},
	)
	printRecords(seqScan)
}

// キーが名前のセカンダリインデックスを使って全件スキャン
func fullIndexScanByFirstName() {
	println("=== インデックススキャン (キーが名前) ===")
	indexScan := executor.NewIndexScan(
		"users",
		"first_name",
		executor.RecordSearchModeStart{},
		func(secondaryKey executor.Record) bool { // フルインデックススキャンなので常に true を返す継続条件
			return true
		},
	)
	printRecords(indexScan)
}

// キーが姓のセカンダリインデックスを使って全件スキャン
func fullIndexScanByLastName() {
	println("=== インデックススキャン (キーが姓) ===")
	indexScan := executor.NewIndexScan(
		"users",
		"last_name",
		executor.RecordSearchModeStart{},
		func(secondaryKey executor.Record) bool {
			return true
		},
	)
	printRecords(indexScan)
}

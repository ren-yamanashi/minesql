package main

import (
	"minesql/internal/executor"
)

// フルテーブルスキャン
func fullTableScan() {
	println("=== フルテーブルスキャン ===")

	// フルテーブルスキャンなので常に true を返す継続条件
	whileCondition := func(record executor.Record) bool {
		return true
	}
	seqScan := executor.NewSequentialScan(
		"users",
		executor.RecordSearchModeStart{},
		whileCondition,
	)
	printRecords(seqScan)
}

// キーが名前のセカンダリインデックスを使って全件スキャン
func fullIndexScanByFirstName() {
	println("=== インデックススキャン (キーが名前) ===")

	// インデックス経由で名前の順序で全件スキャン
	whileCondition := func(secondaryKey executor.Record) bool {
		return true
	}

	indexScan := executor.NewIndexScan(
		"users",
		"first_name",
		executor.RecordSearchModeStart{},
		whileCondition,
	)
	printRecords(indexScan)
}

// キーが姓のセカンダリインデックスを使って全件スキャン
func fullIndexScanByLastName() {
	println("=== インデックススキャン (キーが姓) ===")

	// インデックス経由で姓の順序で全件スキャン
	whileCondition := func(secondaryKey executor.Record) bool {
		return true
	}

	indexScan := executor.NewIndexScan(
		"users",
		"last_name",
		executor.RecordSearchModeStart{},
		whileCondition,
	)
	printRecords(indexScan)
}

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

	for {
		record, err := seqScan.Next()
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

	for {
		record, err := indexScan.Next()
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

	for {
		record, err := indexScan.Next()
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

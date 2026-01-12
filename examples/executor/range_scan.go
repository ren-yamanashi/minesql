package main

import (
	"minesql/internal/executor"
)

// プライマリキーが "w" 以上 "y" 以下の範囲のレコードを取得する (範囲スキャン)
func rangeTableScan() {
	println("=== 範囲スキャン (プライマリキーが 'w' 以上 'y' 以下) ===")

	// プライマリキーが "w" 以上 "y" 以下の間、継続する継続条件
	whileCondition := func(record executor.Record) bool {
		return string(record[0]) <= "y"
	}

	seqScan := executor.NewSequentialScan(
		"users",
		executor.RecordSearchModeKey{Key: [][]byte{[]byte("w")}},
		whileCondition,
	)
	printRecords(seqScan)
}

// 姓が "J" 以上 "N" 未満の範囲のレコードを取得する (インデックス範囲スキャン)
func rangeIndexScan() {
	println("=== インデックス範囲スキャン (姓が 'J' 以上 'N' 未満) ===")

	// インデックス経由で姓が "J" 以上 "N" 未満の範囲をスキャン
	// whileCondition の引数はセカンダリキー (姓) のみ
	whileCondition := func(secondaryKey executor.Record) bool {
		lastName := string(secondaryKey[0])
		return lastName >= "J" && lastName < "N"
	}

	indexScan := executor.NewIndexScan(
		"users",
		"last_name",
		executor.RecordSearchModeKey{Key: [][]byte{[]byte("J")}},
		whileCondition,
	)
	printRecords(indexScan)
}

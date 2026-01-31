package main

import (
	"minesql/internal/executor"
)

// プライマリキーが "w" 以上 "y" 以下の範囲のレコードを取得する (範囲スキャン)
func rangeTableScan() {
	println("=== 範囲スキャン (プライマリキーが 'w' 以上 'y' 以下) ===")
	seqScan := executor.NewSearchTable(
		"users",
		executor.RecordSearchModeKey{Key: [][]byte{[]byte("w")}},
		func(record executor.Record) bool {
			return string(record[0]) <= "y"
		},
	)
	printRecords(seqScan)
}

// 姓が "J" 以上 "N" 未満の範囲のレコードを取得する (インデックス範囲スキャン)
func rangeIndexScan() {
	println("=== インデックス範囲スキャン (姓が 'J' 以上 'N' 未満) ===")
	indexScan := executor.NewSearchIndex(
		"users",
		"idx_last_name",
		executor.RecordSearchModeKey{Key: [][]byte{[]byte("J")}},
		func(secondaryKey executor.Record) bool {
			lastName := string(secondaryKey[0])
			return lastName >= "J" && lastName < "N"
		},
	)
	printRecords(indexScan)
}

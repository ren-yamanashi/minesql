package main

import (
	"minesql/internal/executor"
)

// プライマリキーが "y" のレコードのみを取得する
func searchConstPrimary() {
	println("=== 定数検索 (プライマリキーが 'y') ===")
	seqScan := executor.NewSequentialScan(
		"users",
		executor.RecordSearchModeKey{Key: [][]byte{[]byte("y")}},
		func(record executor.Record) bool {
			return string(record[0]) == "y"
		},
	)
	printRecords(seqScan)
}

// インデックス経由で特定の姓 ("Miller") のレコードのみを取得する
func searchConstUniqueIndex() {
	println("=== ユニークインデックス検索 (姓が 'Miller') ===")
	indexScan := executor.NewIndexScan(
		"users",
		"idx_last_name",
		executor.RecordSearchModeKey{Key: [][]byte{[]byte("Miller")}},
		func(secondaryKey executor.Record) bool {
			return string(secondaryKey[0]) == "Miller"
		},
	)
	printRecords(indexScan)
}

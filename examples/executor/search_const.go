package main

import (
	"minesql/internal/executor"
)

// プライマリキーが "y" のレコードのみを取得する
func searchConstPrimary() {
	println("=== 定数検索 (プライマリキーが 'y') ===")

	// プライマリキーが "y" のレコードのみを取得
	whileCondition := func(record executor.Record) bool {
		return string(record[0]) == "y"
	}

	seqScan := executor.NewSequentialScan(
		"users",
		executor.RecordSearchModeKey{Key: [][]byte{[]byte("y")}},
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

// インデックス経由で特定の姓 ("Miller") のレコードのみを取得する
func searchConstUniqueIndex() {
	println("=== ユニークインデックス検索 (姓が 'Miller') ===")

	// インデックス経由で特定の姓 ("Miller") のレコードを検索
	// whileCondition の引数はセカンダリキー (姓) のみ
	whileCondition := func(secondaryKey executor.Record) bool {
		return string(secondaryKey[0]) == "Miller"
	}

	indexScan := executor.NewIndexScan(
		"users",
		"last_name",
		executor.RecordSearchModeKey{Key: [][]byte{[]byte("Miller")}},
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

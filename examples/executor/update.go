package main

import (
	"fmt"
	"minesql/internal/executor"
)

// レコードの更新を実行する
func updateRecords() {
	fmt.Println("\n=== 更新前のテーブル ===")
	printRecords(executor.NewSearchTable(
		"users",
		executor.RecordSearchModeStart{},
		func(record executor.Record) bool { return true },
	))

	// キーが変わらないケース: first_name が "Alice" のレコードの last_name を更新
	upd := executor.NewUpdate("users", []executor.SetColumn{
		{Pos: 2, Value: []byte("Anderson")},
	}, executor.NewFilter(
		executor.NewSearchTable(
			"users",
			executor.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		),
		func(record executor.Record) bool {
			return string(record[1]) == "Alice"
		},
	))
	printRecords(upd)

	fmt.Println("\n=== 更新後のテーブル (first_name が 'Alice' のレコードの last_name を 'Anderson' に更新) ===")
	printRecords(executor.NewSearchTable(
		"users",
		executor.RecordSearchModeStart{},
		func(record executor.Record) bool { return true },
	))

	fmt.Println("\n=== 更新後のインデックス (last_name 順) ===")
	printRecords(executor.NewSearchIndex(
		"users",
		"idx_last_name",
		executor.RecordSearchModeStart{},
		func(record executor.Record) bool { return true },
	))

	// キーが変わるケース: プライマリキーが "v" のレコードのプライマリキーを "a" に変更
	// (Executor レベルでは SetColumn にプライマリキーのカラムを含めることで実現)
	upd2 := executor.NewUpdate("users", []executor.SetColumn{
		{Pos: 0, Value: []byte("a")},
	}, executor.NewSearchTable(
		"users",
		executor.RecordSearchModeKey{Key: [][]byte{[]byte("v")}},
		func(record executor.Record) bool {
			return string(record[0]) == "v"
		},
	))
	printRecords(upd2)

	fmt.Println("\n=== プライマリキー変更後のテーブル (プライマリキー 'v' を 'a' に変更) ===")
	printRecords(executor.NewSearchTable(
		"users",
		executor.RecordSearchModeStart{},
		func(record executor.Record) bool { return true },
	))
}

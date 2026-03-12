package main

import (
	"fmt"
	"minesql/internal/executor"
)

// レコードの削除を実行する
func deleteRecords() {
	fmt.Println("\n=== 削除前のテーブル ===")
	printRecords(executor.NewSearchTable(
		"users",
		executor.RecordSearchModeStart{},
		func(record executor.Record) bool { return true },
	))

	// first_name が "Bob" のレコードを削除
	del := executor.NewDelete("users", executor.NewFilter(
		executor.NewSearchTable(
			"users",
			executor.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		),
		func(record executor.Record) bool {
			return string(record[1]) == "Bob"
		},
	))
	printRecords(del)

	fmt.Println("\n=== 削除後のテーブル (first_name が \"Bob\" のレコードを削除) ===")
	printRecords(executor.NewSearchTable(
		"users",
		executor.RecordSearchModeStart{},
		func(record executor.Record) bool { return true },
	))

	fmt.Println("\n=== 削除後のインデックス (first_name が \"Bob\" のレコードを削除) (last_name 順) ===")
	printRecords(executor.NewSearchIndex(
		"users",
		"idx_last_name",
		executor.RecordSearchModeStart{},
		func(record executor.Record) bool { return true },
	))
}

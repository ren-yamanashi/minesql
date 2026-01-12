package main

import (
	"minesql/internal/executor"
)

// テーブルを作成し、サンプルデータを挿入する
func createTable() {
	tableName := "users"

	// テーブルを作成
	createTable := executor.NewCreateTable(tableName, 1, []*executor.IndexParam{
		{Name: "first_name", SecondaryKey: 1}, // 名前のインデックス
		{Name: "last_name", SecondaryKey: 2},  // 姓のインデックス
	})
	printRecords(createTable)

	// レコードを挿入
	insert := executor.NewInsert(tableName, [][][]byte{
		{[]byte("z"), []byte("Alice"), []byte("Smith")},
		{[]byte("x"), []byte("Bob"), []byte("Johnson")},
		{[]byte("y"), []byte("Charlie"), []byte("Williams")},
		{[]byte("w"), []byte("Dave"), []byte("Miller")},
		{[]byte("v"), []byte("Eve"), []byte("Brown")},
	})
	printRecords(insert)
}

package main

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access/catalog"
)

// テーブルを作成し、サンプルデータを挿入する
func createTable() {
	tableName := "users"

	// テーブルを作成
	createTable := executor.NewCreateTable(
		tableName,
		1,
		[]*executor.IndexParam{
			{Name: "idx_first_name", ColName: "first_name", SecondaryKey: 1}, // 名前のインデックス
			{Name: "idx_last_name", ColName: "last_name", SecondaryKey: 2},   // 姓のインデックス
		},
		[]*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "first_name", Type: catalog.ColumnTypeString},
			{Name: "last_name", Type: catalog.ColumnTypeString},
		})
	printRecords(createTable)

	// レコードを挿入
	insert := executor.NewInsert(
		tableName,
		[]string{"id", "first_name", "last_name"},
		[][][]byte{
			{[]byte("z"), []byte("Alice"), []byte("Smith")},
			{[]byte("x"), []byte("Bob"), []byte("Johnson")},
			{[]byte("y"), []byte("Charlie"), []byte("Williams")},
			{[]byte("w"), []byte("Dave"), []byte("Miller")},
			{[]byte("v"), []byte("Eve"), []byte("Brown")},
		})

	printRecords(insert)
}

package main

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access/table"
)

// テーブルを作成し、サンプルデータを挿入する
func createTable() {
	tableName := "users"

	// テーブルを作成
	createTable := executor.NewCreateTable()
	err := createTable.Execute(
		tableName,
		1,
		[]*table.UniqueIndex{
			table.NewUniqueIndex("first_name", 1), // 名前のインデックス
			table.NewUniqueIndex("last_name", 2),  // 姓のインデックス
		},
	)
	if err != nil {
		panic(err)
	}

	// レコードを挿入
	insert := executor.NewInsert(tableName)
	err = insert.Execute([][]byte{[]byte("z"), []byte("Alice"), []byte("Smith")})
	if err != nil {
		panic(err)
	}
	err = insert.Execute([][]byte{[]byte("x"), []byte("Bob"), []byte("Johnson")})
	if err != nil {
		panic(err)
	}
	err = insert.Execute([][]byte{[]byte("y"), []byte("Charlie"), []byte("Williams")})
	if err != nil {
		panic(err)
	}
	err = insert.Execute([][]byte{[]byte("w"), []byte("Dave"), []byte("Miller")})
	if err != nil {
		panic(err)
	}
	err = insert.Execute([][]byte{[]byte("v"), []byte("Eve"), []byte("Brown")})
	if err != nil {
		panic(err)
	}
}

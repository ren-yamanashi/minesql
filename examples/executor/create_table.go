package main

import (
	"minesql/internal/executor"
)

// テーブルを作成し、サンプルデータを挿入する
func createTable() {
	tableName := "users"

	// テーブルを作成
	createTable := executor.NewCreateTable()
	err := createTable.Execute(
		tableName,
		1,
		[]*executor.IndexParam{
			{Name: "first_name", SecondaryKey: 1}, // 名前のインデックス
			{Name: "last_name", SecondaryKey: 2},  // 姓のインデックス
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

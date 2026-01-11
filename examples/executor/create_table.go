package main

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/table"
)

// テーブルを作成し、サンプルデータを挿入する
func createTable() {
	engine := storage.GetStorageEngine()

	// テーブルを作成
	tbl, err := engine.CreateTable(
		"users",
		1,
		[]*table.UniqueIndex{
			table.NewUniqueIndex("first_name", 1), // 名前のインデックス
			table.NewUniqueIndex("last_name", 2),  // 姓のインデックス
		},
	)
	if err != nil {
		panic(err)
	}

	bpm := engine.GetBufferPoolManager()

	// レコードを挿入
	err = tbl.Insert(bpm, [][]byte{[]byte("z"), []byte("Alice"), []byte("Smith")})
	if err != nil {
		panic(err)
	}
	err = tbl.Insert(bpm, [][]byte{[]byte("x"), []byte("Bob"), []byte("Johnson")})
	if err != nil {
		panic(err)
	}
	err = tbl.Insert(bpm, [][]byte{[]byte("y"), []byte("Charlie"), []byte("Williams")})
	if err != nil {
		panic(err)
	}
	err = tbl.Insert(bpm, [][]byte{[]byte("w"), []byte("Dave"), []byte("Miller")})
	if err != nil {
		panic(err)
	}
	err = tbl.Insert(bpm, [][]byte{[]byte("v"), []byte("Eve"), []byte("Brown")})
	if err != nil {
		panic(err)
	}

	// バッファプールの内容をディスクにフラッシュ
	err = engine.FlushAll()
	if err != nil {
		panic(err)
	}
}

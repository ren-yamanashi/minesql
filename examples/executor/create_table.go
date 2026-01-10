package main

import (
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

// テーブルを作成し、サンプルデータを挿入する
func createTable(bpm *bufferpool.BufferPoolManager) table.Table {
	// テーブルスキーマの定義
	// インデックス 0: 名前 (2 番目のカラム)
	// インデックス 1: 姓 (3 番目のカラム)
	tbl := table.NewTable(
		disk.OldPageId(0),
		1,
		[]*table.UniqueIndex{
			table.NewUniqueIndex(disk.OLD_INVALID_PAGE_ID, 1), // 名前のインデックス
			table.NewUniqueIndex(disk.OLD_INVALID_PAGE_ID, 2), // 姓のインデックス
		},
	)

	err := tbl.Create(bpm)
	if err != nil {
		panic(err)
	}

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
	err = bpm.FlushPage()
	if err != nil {
		panic(err)
	}

	return tbl
}

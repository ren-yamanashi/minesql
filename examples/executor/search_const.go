package main

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
)

// プライマリキーが "y" のレコードのみを取得する
func searchConstPrimary(bpm *bufferpool.BufferPoolManager, tbl table.Table) {
	println("=== 定数検索 (プライマリキーが 'y') ===")

	// プライマリキーが "y" のレコードのみを取得
	// B+Tree に格納されているキーはエンコードされているため、検索キーもエンコードする
	var encodedKey []byte
	table.Encode([][]byte{[]byte("y")}, &encodedKey)

	whileCondition := func(record executor.Record) bool {
		return string(record[0]) == "y"
	}

	btr := btree.NewBTree(tbl.MetaPageId)
	tableIterator, _ := btr.Search(bpm, btree.SearchModeKey{Key: encodedKey})
	seqScan := executor.NewSequentialScan(
		tableIterator,
		whileCondition,
	)

	for {
		record, err := seqScan.Next(bpm)
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
func searchConstUniqueIndex(bpm *bufferpool.BufferPoolManager, tbl table.Table) {
	println("=== ユニークインデックス検索 (姓が 'Miller') ===")

	// インデックス経由で特定の姓 ("Miller") のレコードを検索
	// WhileCondition の引数はセカンダリキー (姓) のみ
	whileCondition := func(secondaryKey executor.Record) bool {
		return string(secondaryKey[0]) == "Miller"
	}

	lastNameIndexTree := btree.NewBTree(tbl.UniqueIndexes[1].MetaPageId)
	// 姓が "Miller" のキーで検索
	var encodedKey []byte
	table.Encode([][]byte{[]byte("Miller")}, &encodedKey)
	indexIterator, _ := lastNameIndexTree.Search(bpm, btree.SearchModeKey{Key: encodedKey})
	indexScan := executor.NewIndexScan(
		tbl.MetaPageId,
		indexIterator,
		whileCondition,
	)

	for {
		record, err := indexScan.Next(bpm)
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

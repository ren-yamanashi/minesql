package main

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
)

// プライマリキーが "w" 以上 "y" 以下の範囲のレコードを取得する (範囲スキャン)
func rangeTableScan(bpm *bufferpool.BufferPoolManager, tbl table.Table) {
	println("=== 範囲スキャン (プライマリキーが 'w' 以上 'y' 以下) ===")

	// プライマリキーが "w" 以上 "y" 以下の間、継続する継続条件
	whileCondition := func(record executor.Record) bool {
		return string(record[0]) <= "y"
	}
	btr := btree.NewBTree(tbl.MetaPageId)
	tableIterator, _ := btr.Search(bpm, btree.SearchModeKey{Key: []byte("w")})
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

// 姓が "J" 以上 "N" 未満の範囲のレコードを取得する (インデックス範囲スキャン)
func rangeIndexScan(bpm *bufferpool.BufferPoolManager, tbl table.Table) {
	println("=== インデックス範囲スキャン (姓が 'J' 以上 'N' 未満) ===")

	// インデックス経由で姓が "J" 以上 "N" 未満の範囲をスキャン
	// WhileCondition の引数はセカンダリキー (姓) のみ
	whileCondition := func(secondaryKey executor.Record) bool {
		lastName := string(secondaryKey[0])
		return lastName >= "J" && lastName < "N"
	}

	lastNameIndexTree := btree.NewBTree(tbl.UniqueIndexes[1].MetaPageId)
	// 姓が "J" から始まる位置からスキャン開始
	var encodedKey []byte
	table.Encode([][]byte{[]byte("J")}, &encodedKey)
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

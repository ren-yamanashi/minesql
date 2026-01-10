package main

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
)

// フルテーブルスキャン
func fullTableScan(bpm *bufferpool.BufferPoolManager, tbl table.Table) {
	println("=== フルテーブルスキャン ===")

	// フルテーブルスキャンなので常に true を返す継続条件
	whileCondition := func(record executor.Record) bool {
		return true
	}
	btr := btree.NewBTree(tbl.MetaPageId)
	tableIterator, _ := btr.Search(bpm, btree.SearchModeStart{})
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

// キーが名前のセカンダリインデックスを使って全件スキャン
func fullIndexScanByFirstName(bpm *bufferpool.BufferPoolManager, tbl table.Table) {
	println("=== インデックススキャン (キーが名前) ===")

	// インデックス経由で名前の順序で全件スキャン
	// WhileCondition の引数はセカンダリキー (名前) のみ
	whileCondition := func(secondaryKey executor.Record) bool {
		return true
	}

	firstNameIndexTree := btree.NewBTree(tbl.UniqueIndexes[0].MetaPageId)
	indexIterator, _ := firstNameIndexTree.Search(bpm, btree.SearchModeStart{})
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

// キーが姓のセカンダリインデックスを使って全件スキャン
func fullIndexScanByLastName(bpm *bufferpool.BufferPoolManager, tbl table.Table) {
	println("=== インデックススキャン (キーが姓) ===")

	// インデックス経由で姓の順序で全件スキャン
	// WhileCondition の引数はセカンダリキー (姓) のみ
	whileCondition := func(secondaryKey executor.Record) bool {
		return true
	}

	lastNameIndexTree := btree.NewBTree(tbl.UniqueIndexes[1].MetaPageId)
	indexIterator, _ := lastNameIndexTree.Search(bpm, btree.SearchModeStart{})
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

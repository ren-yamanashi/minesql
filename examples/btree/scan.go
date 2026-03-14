package main

import (
	"fmt"

	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"minesql/internal/storage/page"
)

func scan() {
	dataDir := "examples/btree/data"
	dbPath := dataDir + "/test.db"

	bpm := bufferpool.NewBufferPoolManager(10)
	fileId := page.FileId(1)

	// DiskManager を作成して登録
	dm, err := disk.NewDiskManager(fileId, dbPath)
	if err != nil {
		panic(err)
	}
	bpm.RegisterDiskManager(fileId, dm)

	// 既存の B+Tree を開く (MetaPageId は 0 と仮定)
	tree := btree.NewBTree(page.NewPageId(fileId, 0))

	// 全データをスキャン
	scanAll(bpm, tree)
}

// B+Tree の全データをスキャンして表示する
func scanAll(bpm *bufferpool.BufferPoolManager, tree *btree.BTree) {
	iter, err := tree.Search(bpm, btree.SearchModeStart{})
	if err != nil {
		panic(err)
	}

	count := 0
	for {
		pair, ok, err := iter.Next(bpm)
		if err != nil {
			panic(err)
		}
		if !ok {
			break
		}
		fmt.Printf("  key=%-12s value=%s x %d\n", string(pair.Key), string(pair.Value[:1]), len(pair.Value))
		count++
	}
	fmt.Printf("  合計: %d 件\n", count)
}

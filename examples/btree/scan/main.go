package main

import (
	"fmt"

	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"minesql/internal/storage/page"
)

func main() {
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
	iter, err := tree.Search(bpm, btree.SearchModeStart{})
	if err != nil {
		panic(err)
	}

	for {
		pair, ok, err := iter.Next(bpm)
		if err != nil {
			panic(err)
		}
		if !ok {
			break
		}
		valuePreview := string(pair.Value[0]) + " x " + fmt.Sprint(len(pair.Value))
		fmt.Printf("key=%s, value=%s\n", string(pair.Key), valuePreview)
	}
}

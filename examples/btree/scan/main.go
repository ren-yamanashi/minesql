package main

import (
	"fmt"

	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

func main() {
	dbPath := "btree/test.db"

	dm, err := disk.NewDiskManager(dbPath)
	if err != nil {
		panic(err)
	}

	bpm := bufferpool.NewBufferPoolManager(dm, 10)

	// 既存の B+Tree を開く (MetaPageId は 0 と仮定)
	tree := btree.NewBTree(0)

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

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

	// キーで検索
	searchKeys := []string{"grape", "lemon", "watermelon"}
	for _, key := range searchKeys {
		searchMode := btree.SearchModeKey{Key: []byte(key)}
		iter, err := tree.Search(bpm, searchMode)
		if err != nil {
			panic(err)
		}

		pair, ok := iter.Get()
		if ok && string(pair.Key) == key {
			valuePreview := string(pair.Value[0]) + " x " + fmt.Sprint(len(pair.Value))
			fmt.Printf("key=%s, value=%s\n", string(pair.Key), valuePreview)
		} else {
			fmt.Printf("key=%s not found\n", key)
		}
	}
}

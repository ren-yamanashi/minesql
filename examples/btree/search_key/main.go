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

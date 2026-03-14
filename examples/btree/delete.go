package main

import (
	"fmt"
	"strings"

	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

func delete() {
	dataDir := "examples/btree/data"
	dbPath := dataDir + "/delete_test.db"

	bpm := bufferpool.NewBufferPoolManager(10)
	fileId := bpm.AllocateFileId()

	// DiskManager を作成して登録
	dm, err := disk.NewDiskManager(fileId, dbPath)
	if err != nil {
		panic(err)
	}
	bpm.RegisterDiskManager(fileId, dm)

	// metaPageId を割り当て
	metaPageId, err := bpm.AllocatePageId(fileId)
	if err != nil {
		panic(err)
	}

	// B+Tree を作成
	tree, err := btree.CreateBTree(bpm, metaPageId)
	if err != nil {
		panic(err)
	}

	// データを挿入
	fruits := []string{
		"apple", "banana", "cherry", "date", "elderberry",
		"fig", "grape", "honeydew", "kiwi", "lemon",
	}
	for _, fruit := range fruits {
		pair := node.NewPair([]byte(fruit), []byte(strings.Repeat(string(fruit[0]), 100)))
		if err := tree.Insert(bpm, pair); err != nil {
			panic(err)
		}
	}

	fmt.Println("=== 挿入後 ===")
	scanAll(bpm, tree)

	// 一部のキーを削除
	deleteKeys := []string{"banana", "elderberry", "grape"}
	for _, key := range deleteKeys {
		fmt.Printf("\nDelete: %s\n", key)
		if err := tree.Delete(bpm, []byte(key)); err != nil {
			panic(err)
		}
	}

	fmt.Println("\n=== 削除後 ===")
	scanAll(bpm, tree)

	// 存在しないキーを削除してエラーを確認
	fmt.Println("\n=== 存在しないキーの削除 ===")
	err = tree.Delete(bpm, []byte("banana"))
	if err != nil {
		fmt.Printf("期待通りのエラー: %v\n", err)
	}

	// 削除後に新しいキーを挿入できることを確認
	fmt.Println("\n=== 削除後に新しいキーを挿入 ===")
	pair := node.NewPair([]byte("blueberry"), []byte(strings.Repeat("b", 100)))
	if err := tree.Insert(bpm, pair); err != nil {
		panic(err)
	}
	scanAll(bpm, tree)
}

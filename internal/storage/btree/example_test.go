package btree_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"minesql/internal/storage/btree"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

// セットアップヘルパー: B+Tree とバッファプールを作成する
func setupExample() (*btree.BPlusTree, *bufferpool.BufferPool, func()) {
	tmpDir, err := os.MkdirTemp("", "btree_example")
	if err != nil {
		panic(err)
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }

	bp := bufferpool.NewBufferPool(10)
	fileId := bp.AllocateFileId()

	dm, err := disk.NewDisk(fileId, filepath.Join(tmpDir, "example.db"))
	if err != nil {
		panic(err)
	}
	bp.RegisterDisk(fileId, dm)

	metaPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		panic(err)
	}

	tree, err := btree.CreateBPlusTree(bp, metaPageId)
	if err != nil {
		panic(err)
	}

	return tree, bp, cleanup
}

// スキャンヘルパー: B+Tree の全データを表示する
func printAll(bp *bufferpool.BufferPool, tree *btree.BPlusTree) {
	iter, err := tree.Search(bp, btree.SearchModeStart{})
	if err != nil {
		panic(err)
	}

	count := 0
	for {
		pair, ok, err := iter.Next(bp)
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

func ExampleBPlusTree_Insert() {
	tree, bp, cleanup := setupExample()
	defer cleanup()

	// データを挿入
	fruits := []string{"cherry", "apple", "banana", "date", "elderberry"}
	for _, fruit := range fruits {
		pair := btree.NewPair([]byte(fruit), []byte(strings.Repeat(string(fruit[0]), 100)))
		if err := tree.Insert(bp, pair); err != nil {
			panic(err)
		}
	}

	// 全データをスキャン (キー昇順で取得される)
	printAll(bp, tree)

	// Output:
	//   key=apple        value=a x 100
	//   key=banana       value=b x 100
	//   key=cherry       value=c x 100
	//   key=date         value=d x 100
	//   key=elderberry   value=e x 100
	//   合計: 5 件
}

func ExampleBPlusTree_Search() {
	tree, bp, cleanup := setupExample()
	defer cleanup()

	// データを挿入
	for _, fruit := range []string{"apple", "banana", "cherry", "grape", "lemon"} {
		pair := btree.NewPair([]byte(fruit), []byte(strings.Repeat(string(fruit[0]), 100)))
		if err := tree.Insert(bp, pair); err != nil {
			panic(err)
		}
	}

	// キーで検索
	for _, key := range []string{"grape", "lemon", "watermelon"} {
		iter, err := tree.Search(bp, btree.SearchModeKey{Key: []byte(key)})
		if err != nil {
			panic(err)
		}

		pair, ok := iter.Get()
		if ok && string(pair.Key) == key {
			fmt.Printf("key=%s, value=%s x %d\n", string(pair.Key), string(pair.Value[:1]), len(pair.Value))
		} else {
			fmt.Printf("key=%s not found\n", key)
		}
	}

	// Output:
	// key=grape, value=g x 100
	// key=lemon, value=l x 100
	// key=watermelon not found
}

func ExampleBPlusTree_Delete() {
	tree, bp, cleanup := setupExample()
	defer cleanup()

	// データを挿入
	for _, fruit := range []string{"apple", "banana", "cherry", "date", "elderberry"} {
		pair := btree.NewPair([]byte(fruit), []byte(strings.Repeat(string(fruit[0]), 100)))
		if err := tree.Insert(bp, pair); err != nil {
			panic(err)
		}
	}

	fmt.Println("=== 削除前 ===")
	printAll(bp, tree)

	// 一部のキーを削除
	for _, key := range []string{"banana", "date"} {
		if err := tree.Delete(bp, []byte(key)); err != nil {
			panic(err)
		}
	}

	fmt.Println("=== 削除後 ===")
	printAll(bp, tree)

	// 存在しないキーを削除するとエラー
	err := tree.Delete(bp, []byte("banana"))
	fmt.Printf("存在しないキーの削除: %v\n", err)

	// Output:
	// === 削除前 ===
	//   key=apple        value=a x 100
	//   key=banana       value=b x 100
	//   key=cherry       value=c x 100
	//   key=date         value=d x 100
	//   key=elderberry   value=e x 100
	//   合計: 5 件
	// === 削除後 ===
	//   key=apple        value=a x 100
	//   key=cherry       value=c x 100
	//   key=elderberry   value=e x 100
	//   合計: 3 件
	// 存在しないキーの削除: key not found
}

func ExampleBPlusTree_Update() {
	tree, bp, cleanup := setupExample()
	defer cleanup()

	// データを挿入
	for _, fruit := range []string{"apple", "banana", "cherry"} {
		pair := btree.NewPair([]byte(fruit), []byte(strings.Repeat(string(fruit[0]), 100)))
		if err := tree.Insert(bp, pair); err != nil {
			panic(err)
		}
	}

	fmt.Println("=== 更新前 ===")
	printAll(bp, tree)

	// value を更新
	if err := tree.Update(bp, btree.NewPair([]byte("banana"), []byte(strings.Repeat("X", 50)))); err != nil {
		panic(err)
	}

	fmt.Println("=== 更新後 ===")
	printAll(bp, tree)

	// 存在しないキーを更新するとエラー
	err := tree.Update(bp, btree.NewPair([]byte("mango"), []byte("value")))
	fmt.Printf("存在しないキーの更新: %v\n", err)

	// Output:
	// === 更新前 ===
	//   key=apple        value=a x 100
	//   key=banana       value=b x 100
	//   key=cherry       value=c x 100
	//   合計: 3 件
	// === 更新後 ===
	//   key=apple        value=a x 100
	//   key=banana       value=X x 50
	//   key=cherry       value=c x 100
	//   合計: 3 件
	// 存在しないキーの更新: key not found
}

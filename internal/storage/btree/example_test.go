package btree_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

func ExampleTree_Insert() {
	tree, bp, cleanup := setup()
	defer cleanup()

	// データを挿入
	fruits := []string{"cherry", "apple", "banana", "date", "elderberry"}
	for _, fruit := range fruits {
		record := btree.NewRecord(nil, []byte(fruit), []byte(strings.Repeat(string(fruit[0]), 100)))
		mtr := buffer.NewMtr(bp)
		if err := tree.Insert(mtr, record); err != nil {
			panic(err)
		}
		mtr.UnpinAll()
	}

	// 全データをスキャン (キー昇順で取得)
	printAll(tree, bp)

	// Output:
	//   key=apple        value=a x 100
	//   key=banana       value=b x 100
	//   key=cherry       value=c x 100
	//   key=date         value=d x 100
	//   key=elderberry   value=e x 100
	//   合計: 5 件
}

func ExampleTree_Search() {
	tree, bp, cleanup := setup()
	defer cleanup()

	// データを挿入
	for _, fruit := range []string{"apple", "banana", "cherry", "grape", "lemon"} {
		record := btree.NewRecord(nil, []byte(fruit), []byte(strings.Repeat(string(fruit[0]), 100)))
		mtr := buffer.NewMtr(bp)
		if err := tree.Insert(mtr, record); err != nil {
			panic(err)
		}
		mtr.UnpinAll()
	}

	// キーで検索
	for _, key := range []string{"grape", "lemon", "watermelon"} {
		mtr := buffer.NewMtr(bp)
		iter, err := tree.Search(mtr, btree.SearchModeKey{Key: []byte(key)})
		if err != nil {
			panic(err)
		}
		mtr.UnpinAll()

		record, ok, err := iter.Get()
		if err != nil {
			panic(err)
		}
		if ok && string(record.Key()) == key {
			fmt.Printf("key=%s, value=%s x %d\n", string(record.Key()), string(record.NonKey()[:1]), len(record.NonKey()))
		} else {
			fmt.Printf("key=%s not found\n", key)
		}
	}

	// Output:
	// key=grape, value=g x 100
	// key=lemon, value=l x 100
	// key=watermelon not found
}

func ExampleTree_Delete() {
	tree, bp, cleanup := setup()
	defer cleanup()

	// データを挿入
	for _, fruit := range []string{"apple", "banana", "cherry", "date", "elderberry"} {
		record := btree.NewRecord(nil, []byte(fruit), []byte(strings.Repeat(string(fruit[0]), 100)))
		mtr := buffer.NewMtr(bp)
		if err := tree.Insert(mtr, record); err != nil {
			panic(err)
		}
		mtr.UnpinAll()
	}

	fmt.Println("=== 削除前 ===")
	printAll(tree, bp)

	// 一部のキーを削除
	for _, key := range []string{"banana", "date"} {
		mtr := buffer.NewMtr(bp)
		if err := tree.Delete(mtr, []byte(key)); err != nil {
			panic(err)
		}
		mtr.UnpinAll()
	}

	fmt.Println("=== 削除後 ===")
	printAll(tree, bp)

	// 存在しないキーを削除するとエラー
	mtr := buffer.NewMtr(bp)
	err := tree.Delete(mtr, []byte("banana"))
	mtr.UnpinAll()
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

func ExampleTree_Update() {
	tree, bp, cleanup := setup()
	defer cleanup()

	// データを挿入
	for _, fruit := range []string{"apple", "banana", "cherry"} {
		record := btree.NewRecord(nil, []byte(fruit), []byte(strings.Repeat(string(fruit[0]), 100)))
		mtr := buffer.NewMtr(bp)
		if err := tree.Insert(mtr, record); err != nil {
			panic(err)
		}
		mtr.UnpinAll()
	}

	fmt.Println("=== 更新前 ===")
	printAll(tree, bp)

	// value を更新
	updateMtr := buffer.NewMtr(bp)
	if err := tree.Update(updateMtr, btree.NewRecord(nil, []byte("banana"), []byte(strings.Repeat("X", 50)))); err != nil {
		panic(err)
	}
	updateMtr.UnpinAll()

	fmt.Println("=== 更新後 ===")
	printAll(tree, bp)

	// 存在しないキーを更新するとエラー
	mtr := buffer.NewMtr(bp)
	err := tree.Update(mtr, btree.NewRecord(nil, []byte("mango"), []byte("value")))
	mtr.UnpinAll()
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

func setup() (*btree.Tree, *buffer.Pool, func()) {
	tmpDir, err := os.MkdirTemp("", "btree_example")
	if err != nil {
		panic(err)
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }

	rl, err := redo.NewBuffer(tmpDir)
	if err != nil {
		panic(err)
	}

	var bp *buffer.Pool
	bp = buffer.NewPool(page.Size*10, rl, func() {
		_ = bp.FlushOldestPages(bp.FlushListPageCount())
	})
	fileId := page.FileId(1)

	dm, err := file.NewHeapFile(fileId, filepath.Join(tmpDir, "example.db"))
	if err != nil {
		panic(err)
	}
	bp.RegisterHeapFile(fileId, dm)

	tree, err := btree.CreateTree(bp, fileId, rl, lock.SystemReservedTrxId)
	if err != nil {
		panic(err)
	}

	prevCleanup := cleanup
	cleanup = func() {
		_ = rl.Close()
		prevCleanup()
	}

	return tree, bp, cleanup
}

// printAll は B+Tree の全データを表示する
func printAll(tree *btree.Tree, bp *buffer.Pool) {
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	iter, err := tree.Search(mtr, btree.SearchModeStart{})
	if err != nil {
		panic(err)
	}

	count := 0
	for {
		record, ok, err := iter.Next()
		if err != nil {
			panic(err)
		}
		if !ok {
			break
		}
		fmt.Printf("  key=%-12s value=%s x %d\n", string(record.Key()), string(record.NonKey()[:1]), len(record.NonKey()))
		count++
	}
	fmt.Printf("  合計: %d 件\n", count)
}

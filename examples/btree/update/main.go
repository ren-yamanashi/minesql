package main

import (
	"fmt"
	"strings"

	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/btree/node"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

func main() {
	dataDir := "examples/btree/data"
	dbPath := dataDir + "/update_test.db"

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

	// キーが変わらないケース: value のみ更新
	fmt.Println("\n=== value のみ更新 (キーは変わらない) ===")
	updateKeys := []struct {
		key      string
		newValue string
	}{
		{"banana", strings.Repeat("X", 50)},
		{"grape", strings.Repeat("Y", 200)},
		{"lemon", strings.Repeat("Z", 150)},
	}
	for _, u := range updateKeys {
		fmt.Printf("Update: key=%s, newValue=%s x %d\n", u.key, string(u.newValue[0]), len(u.newValue))
		err := tree.Update(bpm, node.NewPair([]byte(u.key), []byte(u.newValue)))
		if err != nil {
			panic(err)
		}
	}
	scanAll(bpm, tree)

	// キーが変わるケース: Delete + Insert で実現
	fmt.Println("\n=== キーを変更する更新 (Delete + Insert) ===")
	keyChanges := []struct {
		oldKey   string
		newKey   string
		newValue string
	}{
		{"apple", "avocado", strings.Repeat("a", 100)},
		{"fig", "feijoa", strings.Repeat("f", 100)},
	}
	for _, kc := range keyChanges {
		fmt.Printf("Update: key=%s → %s\n", kc.oldKey, kc.newKey)
		if err := tree.Delete(bpm, []byte(kc.oldKey)); err != nil {
			panic(err)
		}
		if err := tree.Insert(bpm, node.NewPair([]byte(kc.newKey), []byte(kc.newValue))); err != nil {
			panic(err)
		}
	}
	scanAll(bpm, tree)

	// 存在しないキーの更新でエラーを確認
	fmt.Println("\n=== 存在しないキーの更新 ===")
	err = tree.Update(bpm, node.NewPair([]byte("apple"), []byte("should_fail")))
	if err != nil {
		fmt.Printf("期待通りのエラー: %v\n", err)
	}

	// 同じキーを複数回更新
	fmt.Println("\n=== 同じキーを複数回更新 ===")
	for i := range 3 {
		newValue := fmt.Sprintf("update_%d_%s", i+1, strings.Repeat("!", 50))
		fmt.Printf("Update #%d: key=cherry, value=%s... (%d bytes)\n", i+1, newValue[:20], len(newValue))
		if err := tree.Update(bpm, node.NewPair([]byte("cherry"), []byte(newValue))); err != nil {
			panic(err)
		}
	}
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
		// value が長い場合は先頭部分のみ表示
		valuePreview := string(pair.Value)
		if len(valuePreview) > 30 {
			valuePreview = valuePreview[:30] + "..."
		}
		fmt.Printf("  key=%-12s value=%s (%d bytes)\n", string(pair.Key), valuePreview, len(pair.Value))
		count++
	}
	fmt.Printf("  合計: %d 件\n", count)
}

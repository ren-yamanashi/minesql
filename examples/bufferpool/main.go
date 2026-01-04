package main

import (
	"fmt"
	"os"
	"path/filepath"

	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

func displayBuffer(bpm *bufferpool.BufferPoolManager) {
	fmt.Println("buffer pool status:")
	bp := bpm.GetBufferPool()
	for i, page := range bp.BufferPages {
		ref := 0
		if page.Referenced {
			ref = 1
		}
		dirty := 0
		if page.IsDirty {
			dirty = 1
		}
		fmt.Printf("  Slot %d: PageID=%d, Referenced=%d, IsDirty=%d\n", i, page.PageId, ref, dirty)
	}
	fmt.Printf("  Pointer=%d\n", bp.Pointer)
	fmt.Println()
}

func main() {
	// 一時ディレクトリを作成してデータベースファイルを格納
	tmpDir, err := os.MkdirTemp("", "bufferpool_example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dbPath := filepath.Join(tmpDir, "test.db")

	dm, err := disk.NewDiskManager(dbPath)
	if err != nil {
		panic(err)
	}

	bpm := bufferpool.NewBufferPoolManager(dm, 3)

	// ページを作成
	page1 := dm.AllocatePage()
	page2 := dm.AllocatePage()
	page3 := dm.AllocatePage()
	page4 := dm.AllocatePage()
	page5 := dm.AllocatePage()

	// 各ページにデータを書き込む (PageID と同じ値を書き込む)
	writeTestData(dm, page1, byte(page1))
	writeTestData(dm, page2, byte(page2))
	writeTestData(dm, page3, byte(page3))
	writeTestData(dm, page4, byte(page4))
	writeTestData(dm, page5, byte(page5))

	// ページアクセスのシミュレーション
	fetchAndDisplay(bpm, page1)
	fetchAndDisplay(bpm, page2)
	fetchAndDisplay(bpm, page3)
	fetchAndDisplay(bpm, page1) // ページ 1 を再度アクセス (参照ビットが立つ)
	fetchAndDisplay(bpm, page4) // 新しいページ 4 にアクセス (ページ置換が発生)
	fetchAndDisplay(bpm, page5) // 新しいページ 5 にアクセス (ページ置換が発生)
	fetchAndDisplay(bpm, page1) // ページ 1 を再度アクセス (バッファから追い出されているはず)
}

func writeTestData(dm *disk.DiskManager, pageId disk.PageId, value byte) {
	data := make([]byte, disk.PAGE_SIZE)
	// ページ全体を特定の値で埋める
	for i := range data {
		data[i] = value
	}
	err := dm.WritePageData(pageId, data)
	if err != nil {
		panic(err)
	}
}

func fetchAndDisplay(bpm *bufferpool.BufferPoolManager, pageId disk.PageId) {
	page, err := bpm.FetchPage(pageId)
	if err != nil {
		panic(err)
	}

	// データの検証 (最初の 10 バイトを確認)
	expectedValue := byte(pageId) // writeTestData で書き込んだ値 (PageID と同じ)

	// 期待されるデータを配列形式で作成
	expectedData := make([]byte, 10)
	for i := range expectedData {
		expectedData[i] = expectedValue
	}

	fmt.Printf("fetched page %d\n", pageId)
	fmt.Printf("  expect: %v\n", expectedData)
	fmt.Printf("  actual: %v\n", page.Page[:10])
	displayBuffer(bpm)
}

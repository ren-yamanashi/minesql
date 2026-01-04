package main

import (
	"fmt"
	"os"
	"path/filepath"

	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
)

func displayBuffer(bpm *bufferpool.BufferPoolManager) {
	fmt.Println("現在のバッファプールの状態:")
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
		fmt.Printf("  スロット %d: PageID=%d, 参照ビット=%d, Dirty=%d\n", i, page.PageId, ref, dirty)
	}
	fmt.Printf("  Pointer=%d\n", bp.Pointer)
	fmt.Println()
}

func main() {
	// 一時ディレクトリに DB ファイルを作成
	tmpDir, err := os.MkdirTemp("", "clock-sweep-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	fmt.Printf("データベースファイル: %s\n\n", dbPath)

	// DiskManager を初期化
	dm, err := disk.NewDiskManager(dbPath)
	if err != nil {
		panic(err)
	}

	// 3 スロットの BufferPoolManager を初期化
	bpm := bufferpool.NewBufferPoolManager(dm, 3)
	fmt.Println("3 スロットの BufferPoolManager を初期化しました")

	// ページを作成して書き込み
	page1 := dm.AllocatePage()
	page2 := dm.AllocatePage()
	page3 := dm.AllocatePage()
	page4 := dm.AllocatePage()
	page5 := dm.AllocatePage()

	// 各ページにデータを書き込む
	writeTestData(dm, page1, 1)
	writeTestData(dm, page2, 2)
	writeTestData(dm, page3, 3)
	writeTestData(dm, page4, 4)
	writeTestData(dm, page5, 5)

	// ページアクセスのシミュレーション
	fmt.Println("=== ページ 1 にアクセス ===")
	fetchAndDisplay(bpm, page1)

	fmt.Println("=== ページ 2 にアクセス ===")
	fetchAndDisplay(bpm, page2)

	fmt.Println("=== ページ 3 にアクセス ===")
	fetchAndDisplay(bpm, page3)

	// ページ 1 を再度アクセス (参照ビットが立つ)
	fmt.Println("=== ページ 1 を再度アクセス (参照ビットが立つ) ===")
	fetchAndDisplay(bpm, page1)

	// 新しいページ 4 にアクセス (ページ置換が発生)
	fmt.Println("=== 新しいページ 4 にアクセス (ページ置換が発生) ===")
	fetchAndDisplay(bpm, page4)

	// 新しいページ 5 にアクセス (ページ置換が発生)
	fmt.Println("=== 新しいページ 5 にアクセス (ページ置換が発生) ===")
	fetchAndDisplay(bpm, page5)

	// ページ 1 を再度アクセス (バッファから追い出されているはず)
	fmt.Println("=== ページ 1 を再度アクセス (バッファから追い出されているはず) ===")
	fetchAndDisplay(bpm, page1)

	fmt.Println("Clock-Sweep アルゴリズムの動作確認が完了しました。")
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
	// ページの最初の値を確認 (どのページか識別するため)
	fmt.Printf("ページ %d を取得しました。データの最初のバイト: %d\n", pageId, page.Page[0])
	displayBuffer(bpm)
}

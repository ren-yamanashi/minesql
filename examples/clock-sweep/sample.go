package main

import (
	"fmt"
)

// BufferPage は、バッファプール内の単一のページを表す
type BufferPage struct {
	PageID     int  // ページの一意な識別子
	Referenced bool // 最近アクセスされたことを示す参照ビット
	Dirty      bool // ページが変更されたかどうかを示す
}

// ClockBuffer は、Clock-Sweep アルゴリズムによって管理されるバッファプールを表す
type ClockBuffer struct {
	Pages         []BufferPage // バッファ内のページを格納するスライス
	Pointer       int
	MaxBufferSize int // バッファの最大サイズ
}

// NewClockBuffer は、指定されたサイズの ClockBuffer を初期化する
func NewClockBuffer(size int) *ClockBuffer {
	return &ClockBuffer{
		Pages:         make([]BufferPage, 0, size),
		Pointer:       0,
		MaxBufferSize: size,
	}
}

// AccessPage は、バッファ内のページにアクセスする操作をシミュレートする
func (cb *ClockBuffer) AccessPage(pageID int) {
	for i := range cb.Pages {
		if cb.Pages[i].PageID == pageID {
			cb.Pages[i].Referenced = true
			fmt.Printf("ページ %d にアクセスし、参照ビットを設定しました。\n", pageID)
			return
		}
	}
	fmt.Printf("ページ %d はバッファに存在しません。追加します。\n", pageID)
	cb.AddPage(pageID)
}

// AddPage は、新しいページをバッファに追加し、必要に応じてページを置換する
func (cb *ClockBuffer) AddPage(pageID int) {
	if len(cb.Pages) < cb.MaxBufferSize {
		// バッファに空きがある場合、新しいページを追加
		cb.Pages = append(cb.Pages, BufferPage{PageID: pageID, Referenced: true})
		fmt.Printf("ページ %d をバッファに追加しました。\n", pageID)
	} else {
		// バッファが満杯の場合、ページを置換する
		evictedPage := cb.evictPage()
		fmt.Printf("ページ %d をバッファから置換しました。\n", evictedPage.PageID)
		cb.Pages[cb.Pointer] = BufferPage{PageID: pageID, Referenced: true}
		fmt.Printf("ページ %d をバッファの位置 %d に追加しました。\n", pageID, cb.Pointer)
		cb.advancePointer()
	}
}

// evictPage は、Clock-Sweep アルゴリズムを使用して置換するページを見つける
func (cb *ClockBuffer) evictPage() BufferPage {
	for {
		page := cb.Pages[cb.Pointer]
		if page.Referenced {
			// 参照ビットをクリアし、次のページへ移動
			cb.Pages[cb.Pointer].Referenced = false
			cb.advancePointer()
		} else {
			// 参照ビットがクリアされているページを置換対象とする
			return page
		}
	}
}

func (cb *ClockBuffer) advancePointer() {
	cb.Pointer = (cb.Pointer + 1) % cb.MaxBufferSize
}

func (cb *ClockBuffer) DisplayBuffer() {
	fmt.Println("現在のバッファ状態:")
	for i, page := range cb.Pages {
		ref := 0
		if page.Referenced {
			ref = 1
		}
		fmt.Printf("スロット %d: PageID=%d, 参照ビット=%d\n", i, page.PageID, ref)
	}
	fmt.Println()
}

func main() {
	// 3 スロットの ClockBuffer を初期化
	buffer := NewClockBuffer(3)

	// ページアクセスをシミュレート
	buffer.AccessPage(1)
	buffer.DisplayBuffer()

	buffer.AccessPage(2)
	buffer.DisplayBuffer()

	buffer.AccessPage(3)
	buffer.DisplayBuffer()

	// ページ 1 を再度アクセス（参照ビットがセットされる）
	buffer.AccessPage(1)
	buffer.DisplayBuffer()

	// 新しいページを追加（置換が発生する）
	buffer.AccessPage(4)
	buffer.DisplayBuffer()

	// 別の新しいページを追加（さらにページを置換）
	buffer.AccessPage(5)
	buffer.DisplayBuffer()

	// ページ 1 を再度アクセス（まだバッファに存在するか確認）
	buffer.AccessPage(1)
	buffer.DisplayBuffer()
}

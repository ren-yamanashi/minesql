package bufferpool

import "minesql/internal/storage/disk"

type BufferId uint64

type BufferPool struct {
	BufferPages   []BufferPage
	Pointer       BufferId
	MaxBufferSize int
}

// 指定されたサイズのバッファプールを生成
func NewBufferPool(size int) *BufferPool {
	// メモリ上に空のバッファページを作成
	pages := make([]BufferPage, size)
	for i := range pages {
		pages[i] = BufferPage{
			PageId:     disk.PageId(0),
			Page:       &disk.Page{},
			Referenced: false,
			IsDirty:    false,
		}
	}
	return &BufferPool{
		BufferPages:   pages,
		Pointer:       BufferId(0),
		MaxBufferSize: size,
	}
}

// ポインタを進める
// ポインタがバッファプールの末尾に達した場合、先頭に戻る
func (bp *BufferPool) AdvancePointer() {
	bp.Pointer = (bp.Pointer + 1) % BufferId(bp.MaxBufferSize)
}

// バッファプールから追い出すバッファページを選択する (Clock sweep アルゴリズム)
func (bp *BufferPool) EvictPage() BufferPage {
	for {
		page := bp.BufferPages[bp.Pointer]
		if page.Referenced {
			// 参照ビットをクリアし、次のページへ移動
			bp.BufferPages[bp.Pointer].Referenced = false
			bp.AdvancePointer()
		} else {
			// 参照ビットがクリアされているページを置換対象とする
			return page
		}
	}
}

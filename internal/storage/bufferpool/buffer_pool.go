package bufferpool

import (
	"minesql/internal/storage/page"
)

type BufferId uint64

// BufferPool は、複数のバッファページを管理する (バッファプール)
type BufferPool struct {
	// バッファページのスライス
	BufferPages []BufferPage
	// Clock sweep アルゴリズムで使用するポインタ
	Pointer BufferId
	// バッファプールの最大サイズ (MaxBufferSize * PAGE_SIZE がバッファプールの最大容量)
	MaxBufferSize int
}

// NewBufferPool は指定されたサイズのバッファプールを生成する
func NewBufferPool(size int) *BufferPool {
	// メモリ上に空のバッファページを size 個作成
	pages := make([]BufferPage, size)
	for i := range pages {
		pages[i] = *NewBufferPage(page.INVALID_PAGE_ID) // 仮のページ ID で初期化 (実際にはバッファプールにページが追加されるときに設定される)
	}
	return &BufferPool{
		BufferPages:   pages,
		Pointer:       BufferId(0),
		MaxBufferSize: size,
	}
}

// AdvancePointer はポインタを進める
// ポインタがバッファプールの末尾に達した場合、先頭に戻る
func (bp *BufferPool) AdvancePointer() {
	bp.Pointer = (bp.Pointer + 1) % BufferId(bp.MaxBufferSize)
}

// EvictPage はバッファプールから追い出すバッファページを選択する (Clock sweep アルゴリズム)
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

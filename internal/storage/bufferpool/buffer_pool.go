package bufferpool

import (
	"minesql/internal/storage/page"
)

// BufferPool は、複数のバッファページを管理する (バッファプール)
type BufferPool struct {
	// バッファページのスライス
	BufferPages []BufferPage
	// バッファプールの最大サイズ (指定した数のバッファページを保持できるようになる)
	// つまり `MaxBufferSize(=バッファページ数) * PAGE_SIZE` がバッファプールが使用するメモリ量になる
	MaxBufferSize int
}

// NewBufferPool は指定されたサイズのバッファプールを生成する
// size: バッファページの数 (例: 1000 を指定すると、1000 ページ分のバッファプールが生成される)
func NewBufferPool(size int) *BufferPool {
	pages := allocateBufferPages(size)
	return &BufferPool{
		BufferPages:   pages,
		MaxBufferSize: size,
	}
}

// allocateBufferPages はバッファプール用のメモリ領域を確保する
func allocateBufferPages(size int) []BufferPage {
	// NOTE: 現状は Go のヒープ上にバッファページ用の領域を確保しているが、将来的には OS レベルの共有メモリなどを使用する方針に切り替える可能性がある
	pages := make([]BufferPage, size)
	for i := range pages {
		pages[i] = *NewBufferPage(page.INVALID_PAGE_ID) // 仮のページ ID で初期化 (実際にはバッファプールにページが追加されるときに設定される)
	}
	return pages
}

package bufferPool

import "minesql/internal/storage/disk"

type Pointer uint64

type BufferPool struct {
	BufferPages         []BufferPage
	Pointer       Pointer
	MaxBufferSize int
}

func NewBufferPool(size int) *BufferPool {
	pages := make([]BufferPage, size)
	for i := range pages {
		pages[i] = BufferPage{
			PageId:     disk.PageId(0),
			Page:       &Page{},
			Referenced: false,
			IsDirty:    false,
		}
	}
	return &BufferPool{
		BufferPages:         pages,
		Pointer:       Pointer(0),
		MaxBufferSize: size,
	}
}

// ポインタを進める
// ポインタがバッファプールの末尾に達した場合、先頭に戻る
func (bp *BufferPool) AdvancePointer() {
	bp.Pointer = (bp.Pointer + 1) % Pointer(bp.MaxBufferSize)
}

// バッファプールから追い出すフレームを選択する (Clock sweep アルゴリズム)
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

// // バッファプールからおい出すフレームを選択する (Clock sweep アルゴリズム)
// // 追い出せるバッファが見つかった場合は (FrameId, true) を返し、
// // 見つからなかった場合は (0, false) を返す
// func (bp *BufferPool) evict() (FrameId, bool) {
// 	poolSize := bp.size()
// 	consecutivePinned := 0

// 	for {
// 		nextVictimId := bp.Pointer
// 		frame := bp.getFrame(nextVictimId)

// 		// usageCount が 0 ならば、そのバッファを追い出す
// 		if frame.UsageCount == 0 {
// 			return nextVictimId, true
// 		}

// 		// バッファが使用されているかの確認
// 		if frame.PinCount == 0 {
// 			frame.UsageCount--
// 			consecutivePinned = 0
// 		} else {
// 			// バッファが使用されている場合、連続してピンされているフレーム数をカウントする
// 			consecutivePinned++
// 			if consecutivePinned >= poolSize {
// 				// consecutivePinned が bpSize と同じになってしまった場合は全てのバッファが使用されているということなので、諦めて終了
// 				return 0, false
// 			}
// 		}

// 		// 次のフレームを指すように更新
// 		bp.Pointer = bp.incrementId(nextVictimId)
// 	}
// }

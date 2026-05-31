package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlottedPageHasSpaceFor(t *testing.T) {
	t.Run("ポインタ込みで収まるサイズなら true を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64) // header(8) + usable(56)

		// WHEN
		ok := sp.hasSpaceFor(50) // pointer(4) + data(50) = 54 <= 56

		// THEN
		assert.True(t, ok)
	})

	t.Run("ポインタ込みで超過するサイズなら false を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(20) // header(8) + usable(12)
		sp.insert(0, []byte{1, 2, 3, 4})

		// WHEN
		ok := sp.hasSpaceFor(2) // pointer(4) + data(2) = 6 > 残り 4

		// THEN
		assert.False(t, ok)
	})
}

func TestSlottedPageInsert(t *testing.T) {
	t.Run("データを挿入できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)

		// WHEN
		ok := sp.insert(0, []byte{0xAA, 0xBB})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 1, sp.numSlots())
		assert.Equal(t, []byte{0xAA, 0xBB}, sp.cell(0))
	})

	t.Run("途中のインデックスに挿入するとポインタ配列がシフトされる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01})
		sp.insert(1, []byte{0x03})

		// WHEN
		ok := sp.insert(1, []byte{0x02})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 3, sp.numSlots())
		assert.Equal(t, []byte{0x01}, sp.cell(0))
		assert.Equal(t, []byte{0x02}, sp.cell(1))
		assert.Equal(t, []byte{0x03}, sp.cell(2))
	})

	t.Run("空き容量が不足している場合は false を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(20)     // header(8) + usable(12)
		sp.insert(0, []byte{1, 2, 3, 4}) // pointer(4) + data(4) = 8, remaining 4

		// WHEN
		ok := sp.insert(1, []byte{0xFF})

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 1, sp.numSlots())
	})
}

func TestSlottedPageDelete(t *testing.T) {
	t.Run("データを削除できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0xAA})

		// WHEN
		sp.delete(0)

		// THEN
		assert.Equal(t, 0, sp.numSlots())
	})

	t.Run("途中のインデックスを削除するとポインタ配列がシフトされる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01})
		sp.insert(1, []byte{0x02})
		sp.insert(2, []byte{0x03})

		// WHEN
		sp.delete(1)

		// THEN
		assert.Equal(t, 2, sp.numSlots())
		assert.Equal(t, []byte{0x01}, sp.cell(0))
		assert.Equal(t, []byte{0x03}, sp.cell(1))
	})

	t.Run("末尾のデータを削除できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01})
		sp.insert(1, []byte{0x02})

		// WHEN
		sp.delete(1)

		// THEN
		assert.Equal(t, 1, sp.numSlots())
		assert.Equal(t, []byte{0x01}, sp.cell(0))
	})
}

func TestSlottedPageCanResize(t *testing.T) {
	t.Run("サイズ縮小なら true を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01, 0x02, 0x03, 0x04})

		// WHEN
		ok := sp.canResize(0, 2)

		// THEN
		assert.True(t, ok)
	})

	t.Run("サイズが同じなら true を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01, 0x02, 0x03, 0x04})

		// WHEN
		ok := sp.canResize(0, 4)

		// THEN
		assert.True(t, ok)
	})

	t.Run("サイズ増加分が空き領域に収まるなら true を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01, 0x02})

		// WHEN
		ok := sp.canResize(0, 8) // 増加分 6 <= 空き

		// THEN
		assert.True(t, ok)
	})

	t.Run("サイズ増加分が空き領域を超えるなら false を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(20) // header(8) + usable(12)
		sp.insert(0, []byte{0x01, 0x02})

		// WHEN
		ok := sp.canResize(0, 10) // 増加分 8 > 残り 6

		// THEN
		assert.False(t, ok)
	})
}

func TestSlottedPageUpdate(t *testing.T) {
	t.Run("データを更新できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01, 0x02})

		// WHEN
		ok := sp.update(0, []byte{0xAA, 0xBB})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte{0xAA, 0xBB}, sp.cell(0))
	})

	t.Run("サイズが変わるデータに更新できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01, 0x02})

		// WHEN
		ok := sp.update(0, []byte{0xAA, 0xBB, 0xCC, 0xDD})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte{0xAA, 0xBB, 0xCC, 0xDD}, sp.cell(0))
	})

	t.Run("空き容量が不足している場合は false を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(20)
		sp.insert(0, []byte{1, 2, 3, 4})

		// WHEN
		ok := sp.update(0, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 4, len(sp.cell(0)))
	})
}

func TestSlottedPageResize(t *testing.T) {
	t.Run("データ領域を拡張できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01, 0x02})

		// WHEN
		ok := sp.resize(0, 4)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 4, len(sp.cell(0)))
	})

	t.Run("データ領域を縮小できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01, 0x02, 0x03, 0x04})

		// WHEN
		ok := sp.resize(0, 2)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 2, len(sp.cell(0)))
	})

	t.Run("サイズが同じ場合は何もせず true を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01, 0x02})
		freeSpace := sp.freeSpace()

		// WHEN
		ok := sp.resize(0, 2)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, freeSpace, sp.freeSpace())
	})

	t.Run("空き容量が不足している場合は false を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(20)
		sp.insert(0, []byte{1, 2, 3, 4})

		// WHEN
		ok := sp.resize(0, 100)

		// THEN
		assert.False(t, ok)
	})
}

func TestSlottedPageTransferAllTo(t *testing.T) {
	t.Run("全スロットを転送先に移動できる", func(t *testing.T) {
		// GIVEN
		src := newTestSlottedPage(64)
		src.insert(0, []byte{0x01})
		src.insert(1, []byte{0x02, 0x03})
		dest := newTestSlottedPage(64)

		// WHEN
		ok := src.transferAllTo(dest)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, src.numSlots())
		assert.Equal(t, 2, dest.numSlots())
		assert.Equal(t, []byte{0x01}, dest.cell(0))
		assert.Equal(t, []byte{0x02, 0x03}, dest.cell(1))
	})

	t.Run("ソースが空の場合は true を返す", func(t *testing.T) {
		// GIVEN
		src := newTestSlottedPage(64)
		dest := newTestSlottedPage(64)

		// WHEN
		ok := src.transferAllTo(dest)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, dest.numSlots())
	})

	t.Run("転送先の空き容量が不足している場合は false を返す", func(t *testing.T) {
		// GIVEN
		src := newTestSlottedPage(64)
		src.insert(0, []byte{0x01, 0x02, 0x03})
		dest := newTestSlottedPage(20)
		dest.insert(0, []byte{1, 2, 3, 4}) // remaining: 4

		// WHEN
		ok := src.transferAllTo(dest)

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 1, src.numSlots())
	})

	t.Run("転送先に既存スロットがある場合は末尾に追加される", func(t *testing.T) {
		// GIVEN
		src := newTestSlottedPage(64)
		src.insert(0, []byte{0x03})
		dest := newTestSlottedPage(64)
		dest.insert(0, []byte{0x01})
		dest.insert(1, []byte{0x02})

		// WHEN
		ok := src.transferAllTo(dest)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 3, dest.numSlots())
		assert.Equal(t, []byte{0x01}, dest.cell(0))
		assert.Equal(t, []byte{0x02}, dest.cell(1))
		assert.Equal(t, []byte{0x03}, dest.cell(2))
	})
}

func TestSlottedPageCapacity(t *testing.T) {
	t.Run("ヘッダー領域を除いた容量を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)

		// WHEN / THEN
		assert.Equal(t, 56, sp.capacity()) // 64 - 8(header)
	})
}

func TestSlottedPageNumSlots(t *testing.T) {
	t.Run("挿入したスロット数を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0x01})
		sp.insert(1, []byte{0x02})

		// WHEN / THEN
		assert.Equal(t, 2, sp.numSlots())
	})
}

func TestSlottedPageFreeSpace(t *testing.T) {
	t.Run("初期状態では Capacity と同じ値を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)

		// WHEN / THEN
		assert.Equal(t, sp.capacity(), sp.freeSpace())
	})

	t.Run("挿入後はデータサイズとポインタサイズ分だけ減少する", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		initial := sp.freeSpace()

		// WHEN
		sp.insert(0, []byte{0x01, 0x02, 0x03})

		// THEN
		assert.Equal(t, initial-3-slottedPagePointerSize, sp.freeSpace())
	})
}

func TestSlottedPageCell(t *testing.T) {
	t.Run("指定したインデックスのデータを取得できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.insert(0, []byte{0xAA, 0xBB, 0xCC})

		// WHEN
		cell := sp.cell(0)

		// THEN
		assert.Equal(t, []byte{0xAA, 0xBB, 0xCC}, cell)
	})
}

func TestSlottedPageInitialize(t *testing.T) {
	t.Run("スロット数が 0 になりフリースペースが全容量になる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)

		// WHEN
		sp.initialize()

		// THEN
		assert.Equal(t, 0, sp.numSlots())
		assert.Equal(t, 56, sp.freeSpace()) // 64 - 8(header)
	})
}

// newTestSlottedPage は初期化済みの SlottedPage を作成する
func newTestSlottedPage(size int) *slottedPage {
	data := make([]byte, size)
	sp := newSlottedPage(data)
	sp.initialize()
	return sp
}

package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	t.Run("データを挿入できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)

		// WHEN
		ok := sp.Insert(0, []byte{0xAA, 0xBB})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 1, sp.NumSlots())
		assert.Equal(t, []byte{0xAA, 0xBB}, sp.Cell(0))
	})

	t.Run("途中のインデックスに挿入するとポインタ配列がシフトされる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0x01})
		sp.Insert(1, []byte{0x03})

		// WHEN
		ok := sp.Insert(1, []byte{0x02})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 3, sp.NumSlots())
		assert.Equal(t, []byte{0x01}, sp.Cell(0))
		assert.Equal(t, []byte{0x02}, sp.Cell(1))
		assert.Equal(t, []byte{0x03}, sp.Cell(2))
	})

	t.Run("空き容量が不足している場合は false を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(20)     // header(8) + usable(12)
		sp.Insert(0, []byte{1, 2, 3, 4}) // pointer(4) + data(4) = 8, remaining 4

		// WHEN
		ok := sp.Insert(1, []byte{0xFF})

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 1, sp.NumSlots())
	})
}

func TestRemove(t *testing.T) {
	t.Run("データを削除できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0xAA})

		// WHEN
		sp.Remove(0)

		// THEN
		assert.Equal(t, 0, sp.NumSlots())
	})

	t.Run("途中のインデックスを削除するとポインタ配列がシフトされる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0x01})
		sp.Insert(1, []byte{0x02})
		sp.Insert(2, []byte{0x03})

		// WHEN
		sp.Remove(1)

		// THEN
		assert.Equal(t, 2, sp.NumSlots())
		assert.Equal(t, []byte{0x01}, sp.Cell(0))
		assert.Equal(t, []byte{0x03}, sp.Cell(1))
	})

	t.Run("末尾のデータを削除できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0x01})
		sp.Insert(1, []byte{0x02})

		// WHEN
		sp.Remove(1)

		// THEN
		assert.Equal(t, 1, sp.NumSlots())
		assert.Equal(t, []byte{0x01}, sp.Cell(0))
	})
}

func TestUpdate(t *testing.T) {
	t.Run("データを更新できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0x01, 0x02})

		// WHEN
		ok := sp.Update(0, []byte{0xAA, 0xBB})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte{0xAA, 0xBB}, sp.Cell(0))
	})

	t.Run("サイズが変わるデータに更新できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0x01, 0x02})

		// WHEN
		ok := sp.Update(0, []byte{0xAA, 0xBB, 0xCC, 0xDD})

		// THEN
		assert.True(t, ok)
		assert.Equal(t, []byte{0xAA, 0xBB, 0xCC, 0xDD}, sp.Cell(0))
	})

	t.Run("空き容量が不足している場合は false を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(20)
		sp.Insert(0, []byte{1, 2, 3, 4})

		// WHEN
		ok := sp.Update(0, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 4, len(sp.Cell(0)))
	})
}

func TestResize(t *testing.T) {
	t.Run("データ領域を拡張できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0x01, 0x02})

		// WHEN
		ok := sp.Resize(0, 4)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 4, len(sp.Cell(0)))
	})

	t.Run("データ領域を縮小できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0x01, 0x02, 0x03, 0x04})

		// WHEN
		ok := sp.Resize(0, 2)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 2, len(sp.Cell(0)))
	})

	t.Run("サイズが同じ場合は何もせず true を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0x01, 0x02})
		freeSpace := sp.FreeSpace()

		// WHEN
		ok := sp.Resize(0, 2)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, freeSpace, sp.FreeSpace())
	})

	t.Run("空き容量が不足している場合は false を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(20)
		sp.Insert(0, []byte{1, 2, 3, 4})

		// WHEN
		ok := sp.Resize(0, 100)

		// THEN
		assert.False(t, ok)
	})
}

func TestTransferAllTo(t *testing.T) {
	t.Run("全スロットを転送先に移動できる", func(t *testing.T) {
		// GIVEN
		src := newTestSlottedPage(64)
		src.Insert(0, []byte{0x01})
		src.Insert(1, []byte{0x02, 0x03})
		dest := newTestSlottedPage(64)

		// WHEN
		ok := src.TransferAllTo(dest)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, src.NumSlots())
		assert.Equal(t, 2, dest.NumSlots())
		assert.Equal(t, []byte{0x01}, dest.Cell(0))
		assert.Equal(t, []byte{0x02, 0x03}, dest.Cell(1))
	})

	t.Run("ソースが空の場合は true を返す", func(t *testing.T) {
		// GIVEN
		src := newTestSlottedPage(64)
		dest := newTestSlottedPage(64)

		// WHEN
		ok := src.TransferAllTo(dest)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 0, dest.NumSlots())
	})

	t.Run("転送先の空き容量が不足している場合は false を返す", func(t *testing.T) {
		// GIVEN
		src := newTestSlottedPage(64)
		src.Insert(0, []byte{0x01, 0x02, 0x03})
		dest := newTestSlottedPage(20)
		dest.Insert(0, []byte{1, 2, 3, 4}) // remaining: 4

		// WHEN
		ok := src.TransferAllTo(dest)

		// THEN
		assert.False(t, ok)
		assert.Equal(t, 1, src.NumSlots())
	})

	t.Run("転送先に既存スロットがある場合は末尾に追加される", func(t *testing.T) {
		// GIVEN
		src := newTestSlottedPage(64)
		src.Insert(0, []byte{0x03})
		dest := newTestSlottedPage(64)
		dest.Insert(0, []byte{0x01})
		dest.Insert(1, []byte{0x02})

		// WHEN
		ok := src.TransferAllTo(dest)

		// THEN
		assert.True(t, ok)
		assert.Equal(t, 3, dest.NumSlots())
		assert.Equal(t, []byte{0x01}, dest.Cell(0))
		assert.Equal(t, []byte{0x02}, dest.Cell(1))
		assert.Equal(t, []byte{0x03}, dest.Cell(2))
	})
}

func TestCapacity(t *testing.T) {
	t.Run("ヘッダー領域を除いた容量を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)

		// WHEN / THEN
		assert.Equal(t, 56, sp.Capacity()) // 64 - 8(header)
	})
}

func TestNumSlots(t *testing.T) {
	t.Run("挿入したスロット数を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0x01})
		sp.Insert(1, []byte{0x02})

		// WHEN / THEN
		assert.Equal(t, 2, sp.NumSlots())
	})
}

func TestFreeSpace(t *testing.T) {
	t.Run("初期状態では Capacity と同じ値を返す", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)

		// WHEN / THEN
		assert.Equal(t, sp.Capacity(), sp.FreeSpace())
	})

	t.Run("挿入後はデータサイズとポインタサイズ分だけ減少する", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		initial := sp.FreeSpace()

		// WHEN
		sp.Insert(0, []byte{0x01, 0x02, 0x03})

		// THEN
		assert.Equal(t, initial-3-pointerSize, sp.FreeSpace())
	})
}

func TestCell(t *testing.T) {
	t.Run("指定したインデックスのデータを取得できる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)
		sp.Insert(0, []byte{0xAA, 0xBB, 0xCC})

		// WHEN
		cell := sp.Cell(0)

		// THEN
		assert.Equal(t, []byte{0xAA, 0xBB, 0xCC}, cell)
	})
}

func TestInitialize(t *testing.T) {
	t.Run("スロット数が 0 になりフリースペースが全容量になる", func(t *testing.T) {
		// GIVEN
		sp := newTestSlottedPage(64)

		// WHEN
		sp.Initialize()

		// THEN
		assert.Equal(t, 0, sp.NumSlots())
		assert.Equal(t, 56, sp.FreeSpace()) // 64 - 8(header)
	})
}

// newTestSlottedPage は初期化済みの SlottedPage を作成する
func newTestSlottedPage(size int) *SlottedPage {
	data := make([]byte, size)
	sp := NewSlottedPage(data)
	sp.Initialize()
	return sp
}

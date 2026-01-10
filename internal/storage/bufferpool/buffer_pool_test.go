package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBufferPool(t *testing.T) {
	t.Run("正常にバッファプールが生成される", func(t *testing.T) {
		// GIVEN
		poolSize := 10

		// WHEN
		bp := newBufferPool(poolSize)

		// THEN
		assert.Equal(t, len(bp.BufferPages), poolSize)
		assert.Equal(t, bp.Pointer, BufferId(0))
		assert.Equal(t, bp.MaxBufferSize, poolSize)
	})
}

func TestAdvancePointer(t *testing.T) {
	t.Run("ポインタが正しく進む", func(t *testing.T) {
		// GIVEN
		bp := newBufferPool(5)

		// WHEN
		bp.AdvancePointer()

		// THEN
		assert.Equal(t, bp.Pointer, BufferId(1))
	})

	t.Run("ポインタがバッファプールの末尾に達した場合、先頭に戻る", func(t *testing.T) {
		// GIVEN
		bp := newBufferPool(3)
		bp.Pointer = BufferId(2)

		// WHEN
		bp.AdvancePointer()

		// THEN
		assert.Equal(t, bp.Pointer, BufferId(0))
	})
}

func TestEvictPage(t *testing.T) {
	t.Run("バッファプールから追い出すバッファページが選択される", func(t *testing.T) {
		// GIVEN
		bp := newBufferPool(3)
		bp.BufferPages[0].Referenced = true
		bp.BufferPages[1].Referenced = false
		bp.BufferPages[2].Referenced = false
		bp.Pointer = BufferId(0)

		// WHEN
		evictedPage := bp.EvictPage()

		// THEN
		assert.Equal(t, evictedPage, bp.BufferPages[1])
		assert.Equal(t, bp.Pointer, BufferId(1))
	})

	t.Run("参照ビットがすべて立っている場合、最初のページが追い出される", func(t *testing.T) {
		// GIVEN
		bp := newBufferPool(2)
		bp.BufferPages[0].Referenced = true
		bp.BufferPages[1].Referenced = true
		bp.Pointer = BufferId(0)

		// WHEN
		evictedPage := bp.EvictPage()

		// THEN
		assert.Equal(t, evictedPage, bp.BufferPages[0])
	})
}

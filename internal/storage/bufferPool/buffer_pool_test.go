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
		bufferPool := NewBufferPool(poolSize)

		// THEN
		assert.Equal(t, len(bufferPool.BufferPages), poolSize)
		assert.Equal(t, bufferPool.Pointer, BufferId(0))
		assert.Equal(t, bufferPool.MaxBufferSize, poolSize)
	})
}

func TestAdvancePointer(t *testing.T) {
	t.Run("ポインタが正しく進む", func(t *testing.T) {
		// GIVEN
		bufferPool := NewBufferPool(5)

		// WHEN
		bufferPool.AdvancePointer()

		// THEN
		assert.Equal(t, bufferPool.Pointer, BufferId(1))
	})

	t.Run("ポインタがバッファプールの末尾に達した場合、先頭に戻る", func(t *testing.T) {
		// GIVEN
		bufferPool := NewBufferPool(3)
		bufferPool.Pointer = BufferId(2)

		// WHEN
		bufferPool.AdvancePointer()

		// THEN
		assert.Equal(t, bufferPool.Pointer, BufferId(0))
	})
}

func TestEvictPage(t *testing.T) {
	t.Run("バッファプールから追い出すバッファページが選択される", func(t *testing.T) {
		// GIVEN
		bufferPool := NewBufferPool(3)
		bufferPool.BufferPages[0].Referenced = true
		bufferPool.BufferPages[1].Referenced = false
		bufferPool.BufferPages[2].Referenced = false
		bufferPool.Pointer = BufferId(0)

		// WHEN
		evictedPage := bufferPool.EvictPage()

		// THEN
		assert.Equal(t, evictedPage, bufferPool.BufferPages[1])
		assert.Equal(t, bufferPool.Pointer, BufferId(1))
	})

	t.Run("参照ビットがすべて立っている場合、最初のページが追い出される", func(t *testing.T) {
		// GIVEN
		bufferPool := NewBufferPool(2)
		bufferPool.BufferPages[0].Referenced = true
		bufferPool.BufferPages[1].Referenced = true
		bufferPool.Pointer = BufferId(0)

		// WHEN
		evictedPage := bufferPool.EvictPage()

		// THEN
		assert.Equal(t, evictedPage, bufferPool.BufferPages[0])
	})
}

package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPointerRange(t *testing.T) {
	t.Run("開始位置と終了位置を返す", func(t *testing.T) {
		// GIVEN
		p := newPointer(10, 5)

		// WHEN
		start, end := p.Range()

		// THEN
		assert.Equal(t, 10, start)
		assert.Equal(t, 15, end)
	})

	t.Run("offset が 0 の場合は先頭からの範囲を返す", func(t *testing.T) {
		// GIVEN
		p := newPointer(0, 8)

		// WHEN
		start, end := p.Range()

		// THEN
		assert.Equal(t, 0, start)
		assert.Equal(t, 8, end)
	})

	t.Run("size が 0 の場合は開始位置と終了位置が同じになる", func(t *testing.T) {
		// GIVEN
		p := newPointer(100, 0)

		// WHEN
		start, end := p.Range()

		// THEN
		assert.Equal(t, 100, start)
		assert.Equal(t, 100, end)
	})
}

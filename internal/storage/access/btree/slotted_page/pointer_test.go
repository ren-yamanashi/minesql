package slottedpage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPointer(t *testing.T) {
	t.Run("Pointer インスタンスが生成される", func(t *testing.T) {
		// GIVEN
		offset := uint16(10)
		size := uint16(20)

		// WHEN
		pointer := NewPointer(offset, size)

		// THEN
		assert.Equal(t, offset, pointer.offset)
		assert.Equal(t, size, pointer.size)
	})

	t.Run("正常にポインタの開始位置と終了位置が取得できる", func(t *testing.T) {
		// GIVEN
		offset := uint16(15)
		size := uint16(25)

		// WHEN
		pointer := NewPointer(offset, size)
		start, end := pointer.Range()

		// THEN
		assert.Equal(t, int(offset), start)
		assert.Equal(t, int(offset)+int(size), end)
	})
}

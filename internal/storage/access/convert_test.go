package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringToByteSlice(t *testing.T) {
	t.Run("文字列スライスをバイトスライスに変換できる", func(t *testing.T) {
		// GIVEN
		input := []string{"hello", "world"}

		// WHEN
		result := stringToByteSlice(input)

		// THEN
		assert.Equal(t, [][]byte{[]byte("hello"), []byte("world")}, result)
	})

	t.Run("空のスライスを変換すると空のスライスを返す", func(t *testing.T) {
		// GIVEN
		input := []string{}

		// WHEN
		result := stringToByteSlice(input)

		// THEN
		assert.Equal(t, [][]byte{}, result)
	})

	t.Run("要素が 1 つのスライスを変換できる", func(t *testing.T) {
		// GIVEN
		input := []string{"single"}

		// WHEN
		result := stringToByteSlice(input)

		// THEN
		assert.Equal(t, [][]byte{[]byte("single")}, result)
	})
}

func TestByteSliceToString(t *testing.T) {
	t.Run("バイトスライスを文字列スライスに変換できる", func(t *testing.T) {
		// GIVEN
		input := [][]byte{[]byte("hello"), []byte("world")}

		// WHEN
		result := byteSliceToString(input)

		// THEN
		assert.Equal(t, []string{"hello", "world"}, result)
	})

	t.Run("空のスライスを変換すると空のスライスを返す", func(t *testing.T) {
		// GIVEN
		input := [][]byte{}

		// WHEN
		result := byteSliceToString(input)

		// THEN
		assert.Equal(t, []string{}, result)
	})

	t.Run("要素が 1 つのスライスを変換できる", func(t *testing.T) {
		// GIVEN
		input := [][]byte{[]byte("single")}

		// WHEN
		result := byteSliceToString(input)

		// THEN
		assert.Equal(t, []string{"single"}, result)
	})
}

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodedSize(t *testing.T) {
	t.Run("エンコード後のサイズが正しく計算できる", func(t *testing.T) {
		// GIVEN
		size1 := 20
		size2 := 8
		size3 := 0

		// WHEN
		result1 := EncodedSize(size1)
		result2 := EncodedSize(size2)
		result3 := EncodedSize(size3)

		// THEN
		assert.Equal(t, 27, result1) // 20 バイトは 3 ブロックに分割されるため、3 * 9 = 27 バイト
		assert.Equal(t, 9, result2)  // 8 バイトは 1 ブロックに分割されるため、1 * 9 = 9 バイト
		assert.Equal(t, 0, result3)  // 0 バイトは 0 ブロックのため、0 バイト
	})
}


func TestEncodeToMemcomparable(t *testing.T) {
	t.Run("バイト列が正しく memcomparable 形式にエンコードされる", func(t *testing.T) {
		// GIVEN
		src := []byte("Hello, World!") // 13 バイト
		var destination []byte

		// WHEN
		EncodeToMemcomparable(src, &destination)

		// THEN
		expected := []byte{
			'H', 'e', 'l', 'l', 'o', ',', ' ', 'W', 8,
			'o', 'r', 'l', 'd', '!', 0, 0, 0, 5,
		}
		assert.Equal(t, expected, destination)
	})
}

func TestDecodeFromMemcomparable(t *testing.T) {
	t.Run("memcomparable 形式のバイト列が正しくデコードされる", func(t *testing.T) {
		// GIVEN
		src := []byte{
			'H', 'e', 'l', 'l', 'o', ',', ' ', 'W', 8,
			'o', 'r', 'l', 'd', '!', 0, 0, 0, 5,
		}
		var destination []byte

		// WHEN
		DecodeFromMemcomparable(&src, &destination)

		// THEN
		expected := []byte("Hello, World!")
		assert.Equal(t, expected, destination)
	})
}
package table

import (
	"bytes"
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

	t.Run("複数のバイト列を連続してエンコード/デコードできる", func(t *testing.T) {
		// GIVEN
		src1 := []byte("helloworld!memcmpable")
		src2 := []byte("foobarbazhogehuga")

		var encoded []byte

		// WHEN
		EncodeToMemcomparable(src1, &encoded)
		EncodeToMemcomparable(src2, &encoded)

		var decoded1 []byte
		var decoded2 []byte

		DecodeFromMemcomparable(&encoded, &decoded1)
		DecodeFromMemcomparable(&encoded, &decoded2)

		// THEN
		assert.Equal(t, src1, decoded1)
		assert.Equal(t, src2, decoded2)
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
			'H', 'e', 'l', 'l', 'o', ',', ' ', 'W', 9,
			'o', 'r', 'l', 'd', '!', 0, 0, 0, 5,
		}
		assert.Equal(t, expected, destination)
	})

	t.Run("エンコード後のバイト列を bytes.Compare で比較しても、ソート順が保たれる", func(t *testing.T) {
		testCases := []struct {
			name     string
			a, b     []byte
			expected int // -1: a < b, 0: a == b, 1: a > b
		}{
			{"a < b の場合", []byte("apple"), []byte("banana"), -1},
			{"a > b の場合", []byte("banana"), []byte("apple"), 1},
			{"a == b の場合", []byte("apple"), []byte("apple"), 0},
			{"短い文字列が小さい", []byte("a"), []byte("ab"), -1},
			{"長い文字列が大きい", []byte("ab"), []byte("a"), 1},
			{"空文字列は最小", []byte(""), []byte("a"), -1},
			{"より長いデータ (複数ブロックにまたがる)", []byte("helloworld123"), []byte("helloworld456"), -1},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// WHEN
				var encodedA, encodedB []byte
				EncodeToMemcomparable(tc.a, &encodedA)
				EncodeToMemcomparable(tc.b, &encodedB)

				// 元のバイト列の比較結果
				originalCompare := bytes.Compare(tc.a, tc.b)
				// エンコード後のバイト列の比較結果
				encodedCompare := bytes.Compare(encodedA, encodedB)

				// THEN
				assert.Equal(t, sign(originalCompare), sign(encodedCompare))
			})
		}
	})
}

func TestDecodeFromMemcomparable(t *testing.T) {
	t.Run("memcomparable 形式のバイト列が正しくデコードされる", func(t *testing.T) {
		// GIVEN
		src := []byte{
			'H', 'e', 'l', 'l', 'o', ',', ' ', 'W', 9,
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

func sign(n int) int {
	if n < 0 {
		return -1
	} else if n > 0 {
		return 1
	}
	return 0
}

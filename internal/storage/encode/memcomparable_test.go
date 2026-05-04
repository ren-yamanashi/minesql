package encode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	t.Run("1 バイトのデータをエンコードできる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{0x01}}
		dest := []byte{}

		// WHEN
		Encode(elements, &dest)

		// THEN
		expected := []byte{0x01, 0, 0, 0, 0, 0, 0, 0, 1}
		assert.Equal(t, expected, dest)
	})

	t.Run("8 バイトちょうどのデータをエンコードできる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{1, 2, 3, 4, 5, 6, 7, 8}}
		dest := []byte{}

		// WHEN
		Encode(elements, &dest)

		// THEN
		expected := []byte{1, 2, 3, 4, 5, 6, 7, 8, 8}
		assert.Equal(t, expected, dest)
	})

	t.Run("8 バイトを超えるデータが複数ブロックにエンコードされる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}
		dest := []byte{}

		// WHEN
		Encode(elements, &dest)

		// THEN
		expected := []byte{
			1, 2, 3, 4, 5, 6, 7, 8, 9,
			9, 10, 0, 0, 0, 0, 0, 0, 2,
		}
		assert.Equal(t, expected, dest)
	})

	t.Run("複数の要素を連続してエンコードできる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{0xAA}, {0xBB}}
		dest := []byte{}

		// WHEN
		Encode(elements, &dest)

		// THEN
		expected := []byte{
			0xAA, 0, 0, 0, 0, 0, 0, 0, 1,
			0xBB, 0, 0, 0, 0, 0, 0, 0, 1,
		}
		assert.Equal(t, expected, dest)
	})

	t.Run("dest に既存データがある場合は末尾に追記される", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{0x01}}
		dest := []byte{0xFF}

		// WHEN
		Encode(elements, &dest)

		// THEN
		expected := []byte{0xFF, 0x01, 0, 0, 0, 0, 0, 0, 0, 1}
		assert.Equal(t, expected, dest)
	})

	t.Run("dest の容量が不足している場合は拡張される", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{1, 2, 3, 4, 5, 6, 7, 8}}
		dest := make([]byte, 0, 1)

		// WHEN
		Encode(elements, &dest)

		// THEN
		expected := []byte{1, 2, 3, 4, 5, 6, 7, 8, 8}
		assert.Equal(t, expected, dest)
	})
}

func TestDecode(t *testing.T) {
	t.Run("1 ブロックのデータをデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{0x01, 0, 0, 0, 0, 0, 0, 0, 1}

		// WHEN
		elements := [][]byte{}
		Decode(src, &elements)

		// THEN
		assert.Equal(t, [][]byte{{0x01}}, elements)
	})

	t.Run("8 バイトちょうどのデータをデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{1, 2, 3, 4, 5, 6, 7, 8, 8}

		// WHEN
		elements := [][]byte{}
		Decode(src, &elements)

		// THEN
		assert.Equal(t, [][]byte{{1, 2, 3, 4, 5, 6, 7, 8}}, elements)
	})

	t.Run("複数ブロックにまたがるデータをデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{
			1, 2, 3, 4, 5, 6, 7, 8, 9,
			9, 10, 0, 0, 0, 0, 0, 0, 2,
		}

		// WHEN
		elements := [][]byte{}
		Decode(src, &elements)

		// THEN
		assert.Equal(t, [][]byte{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}, elements)
	})

	t.Run("連続した複数要素をデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{
			0xAA, 0, 0, 0, 0, 0, 0, 0, 1,
			0xBB, 0, 0, 0, 0, 0, 0, 0, 1,
		}

		// WHEN
		elements := [][]byte{}
		Decode(src, &elements)

		// THEN
		assert.Equal(t, [][]byte{{0xAA}, {0xBB}}, elements)
	})

	t.Run("Encode した結果を Decode すると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := [][]byte{
			{1, 2, 3},
			{4, 5, 6, 7, 8, 9, 10, 11, 12},
			{0xFF},
		}
		encoded := []byte{}
		Encode(original, &encoded)

		// WHEN
		decoded := [][]byte{}
		Decode(encoded, &decoded)

		// THEN
		assert.Equal(t, original, decoded)
	})

	t.Run("エンコード後のバイト列がソート順を保つ", func(t *testing.T) {
		// GIVEN
		a := []byte{}
		Encode([][]byte{{1, 0}}, &a)

		b := []byte{}
		Encode([][]byte{{2, 0}}, &b)

		// WHEN / THEN
		// a < b (先頭バイトが 1 < 2)
		assert.Less(t, string(a), string(b))
	})
}

package encode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	t.Run("1 バイトのデータをエンコードできる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{0x01}}

		// WHEN
		dest := Encode(nil, elements)

		// THEN
		expected := []byte{0x01, 0, 0, 0, 0, 0, 0, 0, 1}
		assert.Equal(t, expected, dest)
	})

	t.Run("8 バイトちょうどのデータをエンコードできる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{1, 2, 3, 4, 5, 6, 7, 8}}

		// WHEN
		dest := Encode(nil, elements)

		// THEN
		expected := []byte{1, 2, 3, 4, 5, 6, 7, 8, 8}
		assert.Equal(t, expected, dest)
	})

	t.Run("8 バイトを超えるデータが複数ブロックにエンコードされる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}

		// WHEN
		dest := Encode(nil, elements)

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

		// WHEN
		dest := Encode(nil, elements)

		// THEN
		expected := []byte{
			0xAA, 0, 0, 0, 0, 0, 0, 0, 1,
			0xBB, 0, 0, 0, 0, 0, 0, 0, 1,
		}
		assert.Equal(t, expected, dest)
	})

	t.Run("dst に既存データがある場合は末尾に追記される", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{0x01}}
		dst := []byte{0xFF}

		// WHEN
		dst = Encode(dst, elements)

		// THEN
		expected := []byte{0xFF, 0x01, 0, 0, 0, 0, 0, 0, 0, 1}
		assert.Equal(t, expected, dst)
	})

	t.Run("空の要素をエンコードできる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{}}

		// WHEN
		dest := Encode(nil, elements)

		// THEN
		expected := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0}
		assert.Equal(t, expected, dest)
	})

	t.Run("nil 要素は空のバイト列と同じ扱いになる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{nil}

		// WHEN
		dest := Encode(nil, elements)

		// THEN
		expected := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0}
		assert.Equal(t, expected, dest)
	})

	t.Run("空のスライスの場合は何も書き込まれない", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{}

		// WHEN
		dest := Encode(nil, elements)

		// THEN
		assert.Empty(t, dest)
	})

	t.Run("16 バイトのデータが 2 ブロックにエンコードされる", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}

		// WHEN
		dest := Encode(nil, elements)

		// THEN
		expected := []byte{
			1, 2, 3, 4, 5, 6, 7, 8, 9,
			9, 10, 11, 12, 13, 14, 15, 16, 8,
		}
		assert.Equal(t, expected, dest)
	})

	t.Run("dst の容量が不足している場合は拡張される", func(t *testing.T) {
		// GIVEN
		elements := [][]byte{{1, 2, 3, 4, 5, 6, 7, 8}}
		dst := make([]byte, 0, 1)

		// WHEN
		dst = Encode(dst, elements)

		// THEN
		expected := []byte{1, 2, 3, 4, 5, 6, 7, 8, 8}
		assert.Equal(t, expected, dst)
	})
}

func TestDecode(t *testing.T) {
	t.Run("1 ブロックのデータをデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{0x01, 0, 0, 0, 0, 0, 0, 0, 1}

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{{0x01}}, elements)
	})

	t.Run("8 バイトちょうどのデータをデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{1, 2, 3, 4, 5, 6, 7, 8, 8}

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{{1, 2, 3, 4, 5, 6, 7, 8}}, elements)
	})

	t.Run("複数ブロックにまたがるデータをデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{
			1, 2, 3, 4, 5, 6, 7, 8, 9,
			9, 10, 0, 0, 0, 0, 0, 0, 2,
		}

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}, elements)
	})

	t.Run("連続した複数要素をデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{
			0xAA, 0, 0, 0, 0, 0, 0, 0, 1,
			0xBB, 0, 0, 0, 0, 0, 0, 0, 1,
		}

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{{0xAA}, {0xBB}}, elements)
	})

	t.Run("空の要素をデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0}

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{{}}, elements)
	})

	t.Run("16 バイトのデータをデコードできる", func(t *testing.T) {
		// GIVEN
		src := []byte{
			1, 2, 3, 4, 5, 6, 7, 8, 9,
			9, 10, 11, 12, 13, 14, 15, 16, 8,
		}

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}, elements)
	})

	t.Run("空の src の場合は nil スライスを返す", func(t *testing.T) {
		// GIVEN
		var src []byte

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, elements)
	})

	t.Run("ブロック長に満たない src は ErrInvalidEncoding を返す", func(t *testing.T) {
		// GIVEN
		src := []byte{1, 2, 3}

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidEncoding)
		assert.Nil(t, elements)
	})

	t.Run("長さ情報バイトが continuationMarker より大きい src は ErrInvalidEncoding を返す", func(t *testing.T) {
		// GIVEN
		src := []byte{1, 2, 3, 4, 5, 6, 7, 8, 10}

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidEncoding)
		assert.Nil(t, elements)
	})

	t.Run("継続ブロックの後で src が途切れた場合は ErrInvalidEncoding を返す", func(t *testing.T) {
		// GIVEN
		src := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}

		// WHEN
		elements, err := Decode(src)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidEncoding)
		assert.Nil(t, elements)
	})

	t.Run("Encode した結果を Decode すると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := [][]byte{
			{1, 2, 3},
			{4, 5, 6, 7, 8, 9, 10, 11, 12},
			{0xFF},
		}
		encoded := Encode(nil, original)

		// WHEN
		decoded, err := Decode(encoded)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	})

	t.Run("空要素を含む複数要素をラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		original := [][]byte{{}, {0x01}, {}}
		encoded := Encode(nil, original)

		// WHEN
		decoded, err := Decode(encoded)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, decoded)
	})

	t.Run("エンコード後のバイト列がソート順を保つ (先頭バイトが異なる)", func(t *testing.T) {
		// GIVEN
		a := Encode(nil, [][]byte{{1, 0}})
		b := Encode(nil, [][]byte{{2, 0}})

		// THEN
		assert.Less(t, string(a), string(b))
	})

	t.Run("エンコード後のバイト列がソート順を保つ (短いプレフィックスは小さい)", func(t *testing.T) {
		// GIVEN: a は b のプレフィックス
		a := Encode(nil, [][]byte{{0x01}})
		b := Encode(nil, [][]byte{{0x01, 0x00}})

		// THEN: 短い方が小さい
		assert.Less(t, string(a), string(b))
	})

	t.Run("エンコード後のバイト列がソート順を保つ (ブロック境界をまたぐ)", func(t *testing.T) {
		// GIVEN: 8 バイトちょうど vs 9 バイト
		a := Encode(nil, [][]byte{{1, 2, 3, 4, 5, 6, 7, 8}})
		b := Encode(nil, [][]byte{{1, 2, 3, 4, 5, 6, 7, 8, 0}})

		// THEN: 短い方が小さい
		assert.Less(t, string(a), string(b))
	})

	t.Run("エンコード後のバイト列がソート順を保つ (空 vs 1 バイト)", func(t *testing.T) {
		// GIVEN
		a := Encode(nil, [][]byte{{}})
		b := Encode(nil, [][]byte{{0x00}})

		// THEN: 空の方が小さい
		assert.Less(t, string(a), string(b))
	})
}

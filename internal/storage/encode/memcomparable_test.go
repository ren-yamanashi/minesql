package encode

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	t.Run("複数のバイト列が正しくエンコードされる", func(t *testing.T) {
		// GIVEN
		elements1 := [][]byte{
			[]byte("Hello"),
			[]byte("World!"),
		}
		var destination1 []byte

		elements2 := [][]byte{
			[]byte("A very long byte slice that exceeds eight bytes."),
			[]byte("Short"),
		}
		var destination2 []byte

		// WHEN
		Encode(elements1, &destination1)
		Encode(elements2, &destination2)

		// THEN
		assert.Equal(t, []byte{
			'H', 'e', 'l', 'l', 'o', 0, 0, 0, 5,
			'W', 'o', 'r', 'l', 'd', '!', 0, 0, 6,
		}, destination1)
		assert.Equal(t, []byte{
			'A', ' ', 'v', 'e', 'r', 'y', ' ', 'l', 9,
			'o', 'n', 'g', ' ', 'b', 'y', 't', 'e', 9,
			' ', 's', 'l', 'i', 'c', 'e', ' ', 't', 9,
			'h', 'a', 't', ' ', 'e', 'x', 'c', 'e', 9,
			'e', 'd', 's', ' ', 'e', 'i', 'g', 'h', 9,
			't', ' ', 'b', 'y', 't', 'e', 's', '.', 8,
			'S', 'h', 'o', 'r', 't', 0, 0, 0, 5,
		}, destination2)
	})
}

func TestDecode(t *testing.T) {
	t.Run("エンコードされたバイト列が正しくデコードされる", func(t *testing.T) {
		// GIVEN
		src1 := []byte{
			'H', 'e', 'l', 'l', 'o', 0, 0, 0, 5,
			'W', 'o', 'r', 'l', 'd', '!', 0, 0, 6,
		}

		src2 := []byte{
			'A', ' ', 'v', 'e', 'r', 'y', ' ', 'l', 9,
			'o', 'n', 'g', ' ', 'b', 'y', 't', 'e', 9,
			' ', 's', 'l', 'i', 'c', 'e', ' ', 't', 9,
			'h', 'a', 't', ' ', 'e', 'x', 'c', 'e', 9,
			'e', 'd', 's', ' ', 'e', 'i', 'g', 'h', 9,
			't', ' ', 'b', 'y', 't', 'e', 's', '.', 8,
			'S', 'h', 'o', 'r', 't', 0, 0, 0, 5,
		}

		// WHEN
		var elements1 [][]byte
		var elements2 [][]byte
		Decode(src1, &elements1)
		Decode(src2, &elements2)

		// THEN
		assert.Equal(t, [][]byte{
			[]byte("Hello"),
			[]byte("World!"),
		}, elements1)

		assert.Equal(t, [][]byte{
			[]byte("A very long byte slice that exceeds eight bytes."),
			[]byte("Short"),
		}, elements2)
	})
}

func TestDecodeFirstN(t *testing.T) {
	t.Run("先頭 1 カラムだけデコードし、残りのエンコード済みバイト列が返る", func(t *testing.T) {
		// GIVEN: 3 カラムをエンコード
		var encoded []byte
		Encode([][]byte{[]byte("aaa"), []byte("bbb"), []byte("ccc")}, &encoded)

		// WHEN
		decoded, rest := DecodeFirstN(encoded, 1)

		// THEN
		assert.Equal(t, [][]byte{[]byte("aaa")}, decoded)

		// rest は残り 2 カラムのエンコード済みバイト列
		var restDecoded [][]byte
		Decode(rest, &restDecoded)
		assert.Equal(t, [][]byte{[]byte("bbb"), []byte("ccc")}, restDecoded)
	})

	t.Run("先頭 2 カラムをデコードし、残りが返る", func(t *testing.T) {
		// GIVEN
		var encoded []byte
		Encode([][]byte{[]byte("aaa"), []byte("bbb"), []byte("ccc")}, &encoded)

		// WHEN
		decoded, rest := DecodeFirstN(encoded, 2)

		// THEN
		assert.Equal(t, [][]byte{[]byte("aaa"), []byte("bbb")}, decoded)

		var restDecoded [][]byte
		Decode(rest, &restDecoded)
		assert.Equal(t, [][]byte{[]byte("ccc")}, restDecoded)
	})

	t.Run("全カラムをデコードすると残りが空になる", func(t *testing.T) {
		// GIVEN
		var encoded []byte
		Encode([][]byte{[]byte("aaa"), []byte("bbb")}, &encoded)

		// WHEN
		decoded, rest := DecodeFirstN(encoded, 2)

		// THEN
		assert.Equal(t, [][]byte{[]byte("aaa"), []byte("bbb")}, decoded)
		assert.Equal(t, 0, len(rest))
	})

	t.Run("n がカラム数より大きい場合、全カラムがデコードされ残りが空になる", func(t *testing.T) {
		// GIVEN
		var encoded []byte
		Encode([][]byte{[]byte("aaa")}, &encoded)

		// WHEN
		decoded, rest := DecodeFirstN(encoded, 5)

		// THEN
		assert.Equal(t, [][]byte{[]byte("aaa")}, decoded)
		assert.Equal(t, 0, len(rest))
	})

	t.Run("空のバイト列に対しては空のスライスが返る", func(t *testing.T) {
		// GIVEN & WHEN
		decoded, rest := DecodeFirstN([]byte{}, 1)

		// THEN
		assert.Nil(t, decoded)
		assert.Equal(t, 0, len(rest))
	})

	t.Run("残りのバイト列を再エンコードせずにそのまま btree 検索に使える", func(t *testing.T) {
		// GIVEN: (uniqueKey, pk1, pk2) をエンコード
		var encoded []byte
		Encode([][]byte{[]byte("unique"), []byte("pk1"), []byte("pk2")}, &encoded)

		// WHEN: 先頭 1 カラム (uniqueKey) をデコードし、残り (encodedPK) を取得
		decoded, encodedPK := DecodeFirstN(encoded, 1)

		// THEN: デコードされたユニークキーが正しい
		assert.Equal(t, [][]byte{[]byte("unique")}, decoded)

		// 残りのバイト列は pk1, pk2 を直接 Encode したものと一致する
		var expectedEncodedPK []byte
		Encode([][]byte{[]byte("pk1"), []byte("pk2")}, &expectedEncodedPK)
		assert.Equal(t, expectedEncodedPK, encodedPK)
	})
}

func TestEncodedSize(t *testing.T) {
	t.Run("エンコード後のサイズが正しく計算できる", func(t *testing.T) {
		// GIVEN
		size1 := 20
		size2 := 8
		size3 := 0

		// WHEN
		result1 := encodedSize(size1)
		result2 := encodedSize(size2)
		result3 := encodedSize(size3)

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
		encodeToMemcomparable(src1, &encoded)
		encodeToMemcomparable(src2, &encoded)

		var decoded1 []byte
		var decoded2 []byte

		decodeFromMemcomparable(&encoded, &decoded1)
		decodeFromMemcomparable(&encoded, &decoded2)

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
		encodeToMemcomparable(src, &destination)

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
				encodeToMemcomparable(tc.a, &encodedA)
				encodeToMemcomparable(tc.b, &encodedB)

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
		decodeFromMemcomparable(&src, &destination)

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

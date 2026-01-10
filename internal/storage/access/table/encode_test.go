package table

import (
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

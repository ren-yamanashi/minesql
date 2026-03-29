package access

import (
	"minesql/internal/encode"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRecord(t *testing.T) {
	t.Run("PrimaryKeyCount=1 で Key と NonKey が正しく分離される", func(t *testing.T) {
		// WHEN
		rec := NewRecord([][]byte{[]byte("pk"), []byte("v1"), []byte("v2")}, 1)

		// THEN
		assert.Equal(t, [][]byte{[]byte("pk")}, rec.Key)
		assert.Equal(t, [][]byte{[]byte("v1"), []byte("v2")}, rec.NonKey.NonKeyColumns)
	})

	t.Run("PrimaryKeyCount=2 で複合プライマリキーが正しく分離される", func(t *testing.T) {
		// WHEN
		rec := NewRecord([][]byte{[]byte("pk1"), []byte("pk2"), []byte("v1")}, 2)

		// THEN
		assert.Equal(t, [][]byte{[]byte("pk1"), []byte("pk2")}, rec.Key)
		assert.Equal(t, [][]byte{[]byte("v1")}, rec.NonKey.NonKeyColumns)
	})

	t.Run("NonKey が空の場合でも正しく生成される", func(t *testing.T) {
		// WHEN
		rec := NewRecord([][]byte{[]byte("pk")}, 1)

		// THEN
		assert.Equal(t, [][]byte{[]byte("pk")}, rec.Key)
		assert.Equal(t, 0, len(rec.NonKey.NonKeyColumns))
	})

	t.Run("DeleteMark が 0 で初期化される", func(t *testing.T) {
		// WHEN
		rec := NewRecord([][]byte{[]byte("pk"), []byte("v1")}, 1)

		// THEN
		assert.Equal(t, uint8(0), rec.Header.DeleteMark)
	})
}

func TestColumns(t *testing.T) {
	t.Run("Key と NonKeyColumns を結合したフラット配列を返す", func(t *testing.T) {
		// GIVEN
		rec := NewRecord([][]byte{[]byte("pk"), []byte("v1"), []byte("v2")}, 1)

		// WHEN
		columns := rec.Columns()

		// THEN
		assert.Equal(t, [][]byte{[]byte("pk"), []byte("v1"), []byte("v2")}, columns)
	})

	t.Run("NonKey が空の場合は Key のみ返す", func(t *testing.T) {
		// GIVEN
		rec := NewRecord([][]byte{[]byte("pk")}, 1)

		// WHEN
		columns := rec.Columns()

		// THEN
		assert.Equal(t, [][]byte{[]byte("pk")}, columns)
	})
}

func TestEncodeKey(t *testing.T) {
	t.Run("Key を memcomparable エンコードした結果を返す", func(t *testing.T) {
		// GIVEN
		rec := NewRecord([][]byte{[]byte("pk1"), []byte("v1")}, 1)

		// WHEN
		encoded := rec.EncodeKey()

		// THEN: encode.Encode と同じ結果になる
		var expected []byte
		encode.Encode([][]byte{[]byte("pk1")}, &expected)
		assert.Equal(t, expected, encoded)
	})

	t.Run("複合プライマリキーでも正しくエンコードされる", func(t *testing.T) {
		// GIVEN
		rec := NewRecord([][]byte{[]byte("pk1"), []byte("pk2"), []byte("v1")}, 2)

		// WHEN
		encoded := rec.EncodeKey()

		// THEN
		var expected []byte
		encode.Encode([][]byte{[]byte("pk1"), []byte("pk2")}, &expected)
		assert.Equal(t, expected, encoded)
	})
}

func TestEncodeHeader(t *testing.T) {
	t.Run("DeleteMark=0 の場合 []byte{0} を返す", func(t *testing.T) {
		// GIVEN
		rec := NewRecord([][]byte{[]byte("pk"), []byte("v1")}, 1)

		// WHEN
		header := rec.EncodeHeader()

		// THEN
		assert.Equal(t, []byte{0}, header)
	})

	t.Run("DeleteMark=1 の場合 []byte{1} を返す", func(t *testing.T) {
		// GIVEN
		rec := NewRecord([][]byte{[]byte("pk"), []byte("v1")}, 1)
		rec.Header.DeleteMark = 1

		// WHEN
		header := rec.EncodeHeader()

		// THEN
		assert.Equal(t, []byte{1}, header)
	})
}

func TestEncodeNonKey(t *testing.T) {
	t.Run("NonKeyColumns を memcomparable エンコードした結果を返す", func(t *testing.T) {
		// GIVEN
		rec := NewRecord([][]byte{[]byte("pk"), []byte("v1"), []byte("v2")}, 1)

		// WHEN
		encoded := rec.EncodeNonKey()

		// THEN
		var expected []byte
		encode.Encode([][]byte{[]byte("v1"), []byte("v2")}, &expected)
		assert.Equal(t, expected, encoded)
	})

	t.Run("NonKeyColumns が空の場合は空のエンコード結果を返す", func(t *testing.T) {
		// GIVEN
		rec := NewRecord([][]byte{[]byte("pk")}, 1)

		// WHEN
		encoded := rec.EncodeNonKey()

		// THEN
		var expected []byte
		encode.Encode([][]byte{}, &expected)
		assert.Equal(t, expected, encoded)
	})
}

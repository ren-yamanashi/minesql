package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/stretchr/testify/assert"
)

func TestSecondaryRecordEncode(t *testing.T) {
	t.Run("セカンダリキーとプライマリキーをエンコードしたレコードを返す", func(t *testing.T) {
		// GIVEN
		sr := newSecondaryRecord(1, 0x00, [][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		record := sr.encode()

		// THEN
		assert.Equal(t, []byte{0x00}, record.Header())

		// キー領域: SK + PK が連結されている
		sk, rest := encode.DecodeFirstN(record.Key(), 1)
		assert.Equal(t, [][]byte{[]byte("sk1")}, sk)
		var pk [][]byte
		encode.Decode(rest, &pk)
		assert.Equal(t, [][]byte{[]byte("pk1")}, pk)

		// 非キー領域: 使用しない
		assert.Nil(t, record.NonKey())
	})

	t.Run("複合セカンダリキーを正しくエンコードする", func(t *testing.T) {
		// GIVEN
		sr := newSecondaryRecord(2, 0x00, [][]byte{[]byte("sk1"), []byte("sk2"), []byte("pk1")})

		// WHEN
		record := sr.encode()

		// THEN
		sk, rest := encode.DecodeFirstN(record.Key(), 2)
		assert.Equal(t, [][]byte{[]byte("sk1"), []byte("sk2")}, sk)
		var pk [][]byte
		encode.Decode(rest, &pk)
		assert.Equal(t, [][]byte{[]byte("pk1")}, pk)
	})

	t.Run("削除マークが設定される", func(t *testing.T) {
		// GIVEN
		sr := newSecondaryRecord(1, 0x01, [][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		record := sr.encode()

		// THEN
		assert.Equal(t, []byte{0x01}, record.Header())
	})
}

func TestSecondaryRecordEncodedSecondaryKey(t *testing.T) {
	t.Run("エンコード済みのセカンダリキーのみを返す", func(t *testing.T) {
		// GIVEN
		sr := newSecondaryRecord(1, 0x00, [][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		result := sr.encodedSecondaryKey()

		// THEN
		var expected []byte
		encode.Encode([][]byte{[]byte("sk1")}, &expected)
		assert.Equal(t, expected, result)
	})

	t.Run("複合セカンダリキーの場合も正しくエンコードする", func(t *testing.T) {
		// GIVEN
		sr := newSecondaryRecord(2, 0x00, [][]byte{[]byte("sk1"), []byte("sk2"), []byte("pk1")})

		// WHEN
		result := sr.encodedSecondaryKey()

		// THEN
		var expected []byte
		encode.Encode([][]byte{[]byte("sk1"), []byte("sk2")}, &expected)
		assert.Equal(t, expected, result)
	})
}

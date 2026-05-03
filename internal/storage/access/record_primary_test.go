package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/stretchr/testify/assert"
)

func TestPrimaryRecordEncode(t *testing.T) {
	t.Run("プライマリキーと非キーカラムをエンコードしたレコードを返す", func(t *testing.T) {
		// GIVEN
		pr := newPrimaryRecord(1, 0x00, [][]byte{[]byte("pk1"), []byte("val1")})

		// WHEN
		record := pr.encode()

		// THEN
		assert.Equal(t, []byte{0x00}, record.Header())

		var decodedKey [][]byte
		encode.Decode(record.Key(), &decodedKey)
		assert.Equal(t, [][]byte{[]byte("pk1")}, decodedKey)

		var decodedNonKey [][]byte
		encode.Decode(record.NonKey(), &decodedNonKey)
		assert.Equal(t, [][]byte{[]byte("val1")}, decodedNonKey)
	})

	t.Run("複合プライマリキーを正しくエンコードする", func(t *testing.T) {
		// GIVEN
		pr := newPrimaryRecord(2, 0x00, [][]byte{[]byte("pk1"), []byte("pk2"), []byte("val1")})

		// WHEN
		record := pr.encode()

		// THEN
		var decodedKey [][]byte
		encode.Decode(record.Key(), &decodedKey)
		assert.Equal(t, [][]byte{[]byte("pk1"), []byte("pk2")}, decodedKey)

		var decodedNonKey [][]byte
		encode.Decode(record.NonKey(), &decodedNonKey)
		assert.Equal(t, [][]byte{[]byte("val1")}, decodedNonKey)
	})

	t.Run("削除マークが設定される", func(t *testing.T) {
		// GIVEN
		pr := newPrimaryRecord(1, 0x01, [][]byte{[]byte("pk1"), []byte("val1")})

		// WHEN
		record := pr.encode()

		// THEN
		assert.Equal(t, []byte{0x01}, record.Header())
	})

	t.Run("非キーカラムがない場合も正しくエンコードする", func(t *testing.T) {
		// GIVEN
		pr := newPrimaryRecord(1, 0x00, [][]byte{[]byte("pk1")})

		// WHEN
		record := pr.encode()

		// THEN
		var decodedKey [][]byte
		encode.Decode(record.Key(), &decodedKey)
		assert.Equal(t, [][]byte{[]byte("pk1")}, decodedKey)

		assert.Empty(t, record.NonKey())
	})
}

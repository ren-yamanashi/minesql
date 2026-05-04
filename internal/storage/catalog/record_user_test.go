package catalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserRecordEncode(t *testing.T) {
	t.Run("ユーザーレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		ur := newUserRecord("alice", "localhost", []byte("authdata"))

		// WHEN
		record := ur.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("エンコードした結果をデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := newUserRecord("bob", "192.168.1.1", []byte{0xAB, 0xCD, 0xEF})

		// WHEN
		record := original.encode()
		decoded := decodeUserRecord(record)

		// THEN
		assert.Equal(t, original.Username, decoded.Username)
		assert.Equal(t, original.Host, decoded.Host)
		assert.Equal(t, original.AuthString, decoded.AuthString)
	})

	t.Run("認証文字列が 32 バイトの場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		authString := make([]byte, 32)
		for i := range 32 {
			authString[i] = byte(i)
		}
		original := newUserRecord("user", "%", authString)

		// WHEN
		record := original.encode()
		decoded := decodeUserRecord(record)

		// THEN
		assert.Equal(t, original.Username, decoded.Username)
		assert.Equal(t, original.Host, decoded.Host)
		assert.Equal(t, original.AuthString, decoded.AuthString)
	})

	t.Run("ホスト名がワイルドカードの場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newUserRecord("root", "%", []byte("secret"))

		// WHEN
		record := original.encode()
		decoded := decodeUserRecord(record)

		// THEN
		assert.Equal(t, "root", decoded.Username)
		assert.Equal(t, "%", decoded.Host)
	})
}

func TestDecodeUserRecord(t *testing.T) {
	t.Run("エンコード済みレコードからユーザー名を復元できる", func(t *testing.T) {
		// GIVEN
		ur := newUserRecord("alice", "localhost", []byte("auth"))
		record := ur.encode()

		// WHEN
		decoded := decodeUserRecord(record)

		// THEN
		assert.Equal(t, "alice", decoded.Username)
	})

	t.Run("エンコード済みレコードからホスト名と認証文字列を復元できる", func(t *testing.T) {
		// GIVEN
		ur := newUserRecord("bob", "10.0.0.1", []byte{0x01, 0x02, 0x03})
		record := ur.encode()

		// WHEN
		decoded := decodeUserRecord(record)

		// THEN
		assert.Equal(t, "10.0.0.1", decoded.Host)
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded.AuthString)
	})
}

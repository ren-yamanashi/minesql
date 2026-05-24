package dictionary

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserMetaRecordUsername(t *testing.T) {
	t.Run("コンストラクタで指定したユーザー名を返す", func(t *testing.T) {
		// GIVEN
		ur := NewUserMetaRecord("alice", "localhost", []byte("authdata"))

		// WHEN
		got := ur.Username()

		// THEN
		assert.Equal(t, "alice", got)
	})
}

func TestUserMetaRecordHost(t *testing.T) {
	t.Run("コンストラクタで指定したホスト名を返す", func(t *testing.T) {
		// GIVEN
		ur := NewUserMetaRecord("alice", "localhost", []byte("authdata"))

		// WHEN
		got := ur.Host()

		// THEN
		assert.Equal(t, "localhost", got)
	})
}

func TestUserMetaRecordAuthString(t *testing.T) {
	t.Run("コンストラクタで指定した認証文字列を返す", func(t *testing.T) {
		// GIVEN
		authString := []byte{0xAB, 0xCD, 0xEF}
		ur := NewUserMetaRecord("alice", "localhost", authString)

		// WHEN
		got := ur.AuthString()

		// THEN
		assert.Equal(t, authString, got)
	})
}

func TestUserMetaRecordEncode(t *testing.T) {
	t.Run("ユーザーレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		ur := NewUserMetaRecord("alice", "localhost", []byte("authdata"))

		// WHEN
		record := ur.Encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("エンコードした結果をデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := NewUserMetaRecord("bob", "192.168.1.1", []byte{0xAB, 0xCD, 0xEF})

		// WHEN
		record := original.Encode()
		decoded, err := DecodeUserMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, original.Username(), decoded.Username())
		assert.Equal(t, original.Host(), decoded.Host())
		assert.Equal(t, original.AuthString(), decoded.AuthString())
	})

	t.Run("認証文字列が 32 バイトの場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		authString := make([]byte, 32)
		for i := range 32 {
			authString[i] = byte(i)
		}
		original := NewUserMetaRecord("user", "%", authString)

		// WHEN
		record := original.Encode()
		decoded, err := DecodeUserMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, original.Username(), decoded.Username())
		assert.Equal(t, original.Host(), decoded.Host())
		assert.Equal(t, original.AuthString(), decoded.AuthString())
	})

	t.Run("ホスト名がワイルドカードの場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewUserMetaRecord("root", "%", []byte("secret"))

		// WHEN
		record := original.Encode()
		decoded, err := DecodeUserMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, "root", decoded.Username())
		assert.Equal(t, "%", decoded.Host())
	})
}

func TestDecodeUserMetaRecord(t *testing.T) {
	t.Run("エンコード済みレコードからユーザー名を復元できる", func(t *testing.T) {
		// GIVEN
		ur := NewUserMetaRecord("alice", "localhost", []byte("auth"))
		record := ur.Encode()

		// WHEN
		decoded, err := DecodeUserMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, "alice", decoded.Username())
	})

	t.Run("エンコード済みレコードからホスト名と認証文字列を復元できる", func(t *testing.T) {
		// GIVEN
		ur := NewUserMetaRecord("bob", "10.0.0.1", []byte{0x01, 0x02, 0x03})
		record := ur.Encode()

		// WHEN
		decoded, err := DecodeUserMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, "10.0.0.1", decoded.Host())
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded.AuthString())
	})
}

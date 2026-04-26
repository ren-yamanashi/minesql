package acl

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hexToString(t *testing.T, h string) string {
	t.Helper()
	b, err := hex.DecodeString(h)
	require.NoError(t, err)
	return string(b)
}

func TestCryptPassword(t *testing.T) {
	// WHEN
	hash, err := CryptPassword("testpassword")

	// THEN
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(hash, cryptPrefix))
	// $A$005$ (7) + salt (20) + hash (43) = 70
	assert.Len(t, hash, 7+cryptSaltLen+cryptHashLen)
}

func TestCryptPassword_DifferentSalts(t *testing.T) {
	// WHEN: 同じパスワードで 2 回生成
	hash1, err := CryptPassword("password")
	require.NoError(t, err)

	hash2, err := CryptPassword("password")
	require.NoError(t, err)

	// THEN: ソルトが異なるので異なるハッシュになる
	assert.NotEqual(t, hash1, hash2)
}

func TestVerifyCryptPassword(t *testing.T) {
	// GIVEN: 自前で生成したハッシュ
	password := "mySecretPassword123"
	hash, err := CryptPassword(password)
	require.NoError(t, err)

	// WHEN/THEN: 正しいパスワードで検証成功
	assert.True(t, VerifyCryptPassword(password, hash))

	// WHEN/THEN: 不正なパスワードで検証失敗
	assert.False(t, VerifyCryptPassword("wrongPassword", hash))
}

func TestVerifyCryptPassword_MySQLGenerated(t *testing.T) {
	// GIVEN: MySQL が生成した $A$005$ ハッシュ (hex から復元)
	// CREATE USER 'test_crypt'@'%' IDENTIFIED BY 'testpassword123';
	// ソルトに制御文字が含まれるため hex で保持する
	mysqlHashHex := "24412430303524393D02673221785B414A4E5F02073711301659116B446B76753345617A4D7530635548526D3678746F6B75564835553334346932572E332F704C4856477131"
	mysqlHash := hexToString(t, mysqlHashHex)

	// WHEN/THEN: 正しいパスワードで検証成功
	assert.True(t, VerifyCryptPassword("testpassword123", mysqlHash))

	// WHEN/THEN: 不正なパスワードで検証失敗
	assert.False(t, VerifyCryptPassword("wrongpassword", mysqlHash))
}

func TestVerifyCryptPassword_InvalidFormat(t *testing.T) {
	// WHEN/THEN: プレフィックスが異なる場合は失敗
	assert.False(t, VerifyCryptPassword("password", "$B$005$somesaltvalue12345678somehashvalue12345678901234567890abc"))

	// WHEN/THEN: 長さが不正な場合は失敗
	assert.False(t, VerifyCryptPassword("password", "$A$005$short"))

	// WHEN/THEN: 空文字列は失敗
	assert.False(t, VerifyCryptPassword("password", ""))
}

func TestVerifyCryptPassword_EmptyPassword(t *testing.T) {
	// GIVEN: 空パスワードのハッシュ
	hash, err := CryptPassword("")
	require.NoError(t, err)

	// WHEN/THEN
	assert.True(t, VerifyCryptPassword("", hash))
	assert.False(t, VerifyCryptPassword("notempty", hash))
}

func TestGenerateSalt(t *testing.T) {
	// WHEN
	salt, err := generateSalt(cryptSaltLen)

	// THEN
	require.NoError(t, err)
	assert.Len(t, salt, cryptSaltLen)

	for i, b := range salt {
		// 7 ビット ASCII
		assert.LessOrEqual(t, b, byte(0x7F), "byte %d: 0x%02X is not 7-bit ASCII", i, b)
		// NUL と '$' は含まない
		assert.NotEqual(t, byte(0), b, "byte %d: must not be NUL", i)
		assert.NotEqual(t, byte('$'), b, "byte %d: must not be '$'", i)
	}
}

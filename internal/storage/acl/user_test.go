package acl

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewUser(t *testing.T) {
	t.Run("パスワードから AuthString が正しく計算される", func(t *testing.T) {
		// GIVEN
		password := "mypassword"

		// WHEN
		user := NewUser("root", password, "%")

		// THEN
		stage1 := sha256.Sum256([]byte(password))
		expected := sha256.Sum256(stage1[:])
		assert.Equal(t, "root", user.Username)
		assert.Equal(t, "%", user.Host)
		assert.Equal(t, expected, user.AuthString)
	})

	t.Run("空パスワードでも AuthString が計算される", func(t *testing.T) {
		// WHEN
		user := NewUser("root", "", "%")

		// THEN
		stage1 := sha256.Sum256([]byte(""))
		expected := sha256.Sum256(stage1[:])
		assert.Equal(t, expected, user.AuthString)
	})

	t.Run("同じパスワードなら同じ AuthString になる", func(t *testing.T) {
		// WHEN
		user1 := NewUser("alice", "pass", "%")
		user2 := NewUser("bob", "pass", "127.0.0.1")

		// THEN
		assert.Equal(t, user1.AuthString, user2.AuthString)
	})

	t.Run("異なるパスワードなら異なる AuthString になる", func(t *testing.T) {
		// WHEN
		user1 := NewUser("root", "pass1", "%")
		user2 := NewUser("root", "pass2", "%")

		// THEN
		assert.NotEqual(t, user1.AuthString, user2.AuthString)
	})
}

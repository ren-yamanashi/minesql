package handler

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateUser(t *testing.T) {
	t.Run("ユーザーを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		authString := computeHandlerTestAuthString("mypassword")

		// WHEN
		err := h.CreateUser("root", "%", authString)

		// THEN
		assert.NoError(t, err)
		assert.True(t, h.Catalog.HasUsers())
		user := h.Catalog.Users[0]
		assert.Equal(t, "root", user.Username)
		assert.Equal(t, "%", user.Host)
		assert.Equal(t, authString, user.AuthString)
	})

	t.Run("ホスト付きのユーザーを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		authString := computeHandlerTestAuthString("secret")

		// WHEN
		err := h.CreateUser("admin", "192.168.1.%", authString)

		// THEN
		assert.NoError(t, err)
		user := h.Catalog.Users[0]
		assert.Equal(t, "admin", user.Username)
		assert.Equal(t, "192.168.1.%", user.Host)
		assert.Equal(t, authString, user.AuthString)
	})
}

func computeHandlerTestAuthString(password string) [32]byte {
	stage1 := sha256.Sum256([]byte(password))
	return sha256.Sum256(stage1[:])
}

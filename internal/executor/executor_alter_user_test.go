package executor

import (
	"minesql/internal/storage/acl"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlterUser_Next(t *testing.T) {
	t.Run("ユーザーの認証情報を更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()

		oldAuthString := cryptAlterUserTestPassword(t, "oldpass")
		err := hdl.CreateUser("root", "%", oldAuthString)
		assert.NoError(t, err)

		// WHEN
		newAuthString := cryptAlterUserTestPassword(t, "newpass")
		alterUser := NewAlterUser("root", "%", newAuthString)
		_, err = alterUser.Next()

		// THEN
		assert.NoError(t, err)
		user, ok := hdl.Catalog.GetUserByName("root")
		assert.True(t, ok)
		assert.Equal(t, newAuthString, user.AuthString)
	})

	t.Run("存在しないユーザーの更新はエラーになる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()

		// WHEN
		authString := cryptAlterUserTestPassword(t, "pass")
		alterUser := NewAlterUser("nonexistent", "%", authString)
		_, err := alterUser.Next()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "user 'nonexistent' not found")
	})
}

func cryptAlterUserTestPassword(t *testing.T, password string) string {
	t.Helper()
	s, err := acl.CryptPassword(password)
	require.NoError(t, err)
	return s
}

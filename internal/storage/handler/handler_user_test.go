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

	t.Run("ユーザー作成時に ACL が構築される", func(t *testing.T) {
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
		assert.NotNil(t, h.ACL)
		user, ok := h.ACL.Lookup("127.0.0.1", "root")
		assert.True(t, ok)
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

func TestUpdateUser(t *testing.T) {
	t.Run("ユーザーの認証情報を更新できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		oldAuthString := computeHandlerTestAuthString("oldpassword")
		err := h.CreateUser("root", "%", oldAuthString)
		assert.NoError(t, err)

		// WHEN
		newAuthString := computeHandlerTestAuthString("newpassword")
		err = h.UpdateUser("root", "%", newAuthString)

		// THEN
		assert.NoError(t, err)
		user, ok := h.Catalog.GetUserByName("root")
		assert.True(t, ok)
		assert.Equal(t, newAuthString, user.AuthString)
	})

	t.Run("更新後に ACL が再構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		oldAuthString := computeHandlerTestAuthString("oldpassword")
		err := h.CreateUser("root", "%", oldAuthString)
		assert.NoError(t, err)

		// WHEN
		newAuthString := computeHandlerTestAuthString("newpassword")
		err = h.UpdateUser("root", "%", newAuthString)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, h.ACL)
		user, ok := h.ACL.Lookup("127.0.0.1", "root")
		assert.True(t, ok)
		assert.Equal(t, newAuthString, user.AuthString)
	})

	t.Run("存在しないユーザーの更新はエラーになる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		authString := computeHandlerTestAuthString("password")
		err := h.UpdateUser("nonexistent", "%", authString)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "user 'nonexistent' not found")
	})
}

func TestLoadACL(t *testing.T) {
	t.Run("カタログにユーザーが存在する場合は ACL を構築して true を返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		authString := computeHandlerTestAuthString("password")
		err := h.CreateUser("root", "%", authString)
		assert.NoError(t, err)

		// ACL をリセットして LoadACL をテスト
		h.ACL = nil

		// WHEN
		ok := h.LoadACL()

		// THEN
		assert.True(t, ok)
		assert.NotNil(t, h.ACL)
		user, found := h.ACL.Lookup("127.0.0.1", "root")
		assert.True(t, found)
		assert.Equal(t, "root", user.Username)
	})

	t.Run("カタログにユーザーが存在しない場合は false を返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		ok := h.LoadACL()

		// THEN
		assert.False(t, ok)
		assert.Nil(t, h.ACL)
	})
}

func TestCreateInitialUser(t *testing.T) {
	t.Run("初期ユーザーを作成してランダムパスワードを返す", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		password, err := h.CreateInitialUser("root", "%")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 16, len(password))
		assert.True(t, h.Catalog.HasUsers())
		assert.NotNil(t, h.ACL)

		user, ok := h.ACL.Lookup("127.0.0.1", "root")
		assert.True(t, ok)
		assert.Equal(t, "root", user.Username)
	})

	t.Run("生成されるパスワードが毎回異なる", func(t *testing.T) {
		// GIVEN / WHEN
		tmpdir1 := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir1)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h1 := Init()
		p1, err := h1.CreateInitialUser("root", "%")
		assert.NoError(t, err)

		tmpdir2 := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir2)
		Reset()
		h2 := Init()
		p2, err := h2.CreateInitialUser("root", "%")
		assert.NoError(t, err)

		// THEN
		assert.NotEqual(t, p1, p2)
	})
}

func computeHandlerTestAuthString(password string) [32]byte {
	stage1 := sha256.Sum256([]byte(password))
	return sha256.Sum256(stage1[:])
}

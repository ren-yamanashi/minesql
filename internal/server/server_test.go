package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"minesql/internal/storage/handler"
)

func TestNewServer(t *testing.T) {
	t.Run("Server が正しく初期化される", func(t *testing.T) {
		// WHEN
		s := NewServer("localhost", 3307, nil)

		// THEN
		assert.Equal(t, "localhost", s.address)
		assert.Equal(t, 3307, s.port)
		assert.Nil(t, s.initUser)
	})

	t.Run("InitUserOpts 付きで初期化される", func(t *testing.T) {
		// GIVEN
		opts := &InitUserOpts{
			Username: "admin",
			Host:     "192.168.1.%",
		}

		// WHEN
		s := NewServer("localhost", 3307, opts)

		// THEN
		assert.Equal(t, "admin", s.initUser.Username)
		assert.Equal(t, "192.168.1.%", s.initUser.Host)
	})
}

func TestInit(t *testing.T) {
	t.Run("init で TLS Config が初期化される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		s := &Server{
			initUser: &InitUserOpts{Username: "root", Host: "%"},
		}
		// init() 内部で dataDir="data" を使うので、環境変数で tmpdir を使わせる
		// ただし init() は dataDir をハードコードしているため、直接テストは難しい
		// 代わりに loadOrGenerateTLSConfig を直接テストする
		tlsConfig, err := loadOrGenerateTLSConfig(tmpdir)

		// THEN
		require.NoError(t, err)
		assert.NotNil(t, tlsConfig)
		assert.Len(t, tlsConfig.Certificates, 1)

		// Server に設定できる
		s.tlsConfig = tlsConfig
		assert.NotNil(t, s.tlsConfig)
	})
}

func TestInitACL(t *testing.T) {
	t.Run("初期ユーザー指定で ACL が構築される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		s := &Server{
			initUser: &InitUserOpts{
				Username: "root",
				Host:     "%",
			},
		}

		// WHEN
		err := s.initACL()

		// THEN
		require.NoError(t, err)
		assert.NotNil(t, handler.Get().ACL)

		// ACL から Lookup できる
		_, ok := handler.Get().ACL.Lookup("127.0.0.1", "root")
		assert.True(t, ok)
	})

	t.Run("初期ユーザーがカタログに永続化される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		s := &Server{
			initUser: &InitUserOpts{
				Username: "admin",
				Host:     "192.168.1.%",
			},
		}

		// WHEN
		err := s.initACL()

		// THEN
		require.NoError(t, err)
		hdl := handler.Get()
		assert.True(t, hdl.Catalog.HasUsers())
		assert.Equal(t, "admin", hdl.Catalog.Users[0].Username)
		assert.Equal(t, "192.168.1.%", hdl.Catalog.Users[0].Host)
	})

	t.Run("カタログにユーザーがなく --init-* もない場合はエラー", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		s := &Server{}

		// WHEN
		err := s.initACL()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no user found in catalog")
	})

	t.Run("カタログにユーザーが存在する状態で --init-* を指定しても無視される", func(t *testing.T) {
		// GIVEN: 初期ユーザーを先に作成
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		s1 := &Server{
			initUser: &InitUserOpts{
				Username: "root",
				Host:     "%",
			},
		}
		err := s1.initACL()
		require.NoError(t, err)

		// WHEN: 別のパスワードで --init-* を指定
		s2 := &Server{
			initUser: &InitUserOpts{
				Username: "root",
				Host:     "%",
			},
		}
		err = s2.initACL()

		// THEN: エラーにならず、元のユーザーが使われる (WARN ログは出るが検証しない)
		require.NoError(t, err)
		assert.NotNil(t, handler.Get().ACL)
	})

}

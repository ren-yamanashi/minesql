package acl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchHost(t *testing.T) {
	t.Run("完全一致", func(t *testing.T) {
		assert.True(t, MatchHost("192.168.1.100", "192.168.1.100"))
		assert.False(t, MatchHost("192.168.1.100", "192.168.1.101"))
	})

	t.Run("% は全ホストにマッチ", func(t *testing.T) {
		assert.True(t, MatchHost("%", "192.168.1.100"))
		assert.True(t, MatchHost("%", "127.0.0.1"))
		assert.True(t, MatchHost("%", "10.0.0.1"))
	})

	t.Run("サブネットパターン", func(t *testing.T) {
		assert.True(t, MatchHost("192.168.1.%", "192.168.1.100"))
		assert.True(t, MatchHost("192.168.1.%", "192.168.1.1"))
		assert.False(t, MatchHost("192.168.1.%", "192.168.2.100"))
		assert.False(t, MatchHost("192.168.1.%", "10.0.0.1"))
	})

	t.Run("上位オクテットのサブネットパターン", func(t *testing.T) {
		assert.True(t, MatchHost("10.%", "10.0.0.1"))
		assert.True(t, MatchHost("10.%", "10.255.255.255"))
		assert.False(t, MatchHost("10.%", "192.168.1.1"))
	})

	t.Run("2 オクテットのサブネットパターン", func(t *testing.T) {
		assert.True(t, MatchHost("192.168.%", "192.168.1.1"))
		assert.True(t, MatchHost("192.168.%", "192.168.255.255"))
		assert.False(t, MatchHost("192.168.%", "192.169.1.1"))
	})

	t.Run("localhost の完全一致", func(t *testing.T) {
		assert.True(t, MatchHost("127.0.0.1", "127.0.0.1"))
		assert.False(t, MatchHost("127.0.0.1", "127.0.0.2"))
	})

	t.Run("空文字列のホスト", func(t *testing.T) {
		assert.False(t, MatchHost("192.168.1.100", ""))
		assert.True(t, MatchHost("%", ""))
	})
}

func TestNewACLFromCatalog(t *testing.T) {
	t.Run("カタログ情報から ACL を構築できる", func(t *testing.T) {
		// GIVEN
		authString, err := CryptPassword("mypass")
		require.NoError(t, err)

		// WHEN
		a := NewACLFromCatalog("admin", "192.168.1.%", authString)

		// THEN
		foundAuthString, ok := a.Lookup("192.168.1.50", "admin")
		assert.True(t, ok)
		assert.Equal(t, authString, foundAuthString)
	})

	t.Run("カタログから構築した ACL でホスト不一致は見つからない", func(t *testing.T) {
		// GIVEN
		authString, err := CryptPassword("pass")
		require.NoError(t, err)
		a := NewACLFromCatalog("admin", "192.168.1.%", authString)

		// WHEN
		_, ok := a.Lookup("10.0.0.1", "admin")

		// THEN
		assert.False(t, ok)
	})

	t.Run("カタログから構築した ACL でユーザー名不一致は見つからない", func(t *testing.T) {
		// GIVEN
		authString, err := CryptPassword("pass")
		require.NoError(t, err)
		a := NewACLFromCatalog("admin", "%", authString)

		// WHEN
		_, ok := a.Lookup("127.0.0.1", "root")

		// THEN
		assert.False(t, ok)
	})
}

func TestLookup(t *testing.T) {
	t.Run("ユーザー名とホストが一致する場合に見つかる", func(t *testing.T) {
		// GIVEN
		a := testACL(t, "pass", "%")

		// WHEN
		_, ok := a.Lookup("192.168.1.100", "root")

		// THEN
		assert.True(t, ok)
	})

	t.Run("ユーザー名が一致しない場合は見つからない", func(t *testing.T) {
		// GIVEN
		a := testACL(t, "pass", "%")

		// WHEN
		_, ok := a.Lookup("192.168.1.100", "unknown")

		// THEN
		assert.False(t, ok)
	})

	t.Run("ホストが一致しない場合は見つからない", func(t *testing.T) {
		// GIVEN
		a := testACL(t, "pass", "127.0.0.1")

		// WHEN
		_, ok := a.Lookup("192.168.1.100", "root")

		// THEN
		assert.False(t, ok)
	})

	t.Run("サブネットパターンでホストがマッチする", func(t *testing.T) {
		// GIVEN
		a := testACL(t, "pass", "192.168.1.%")

		// WHEN
		_, ok := a.Lookup("192.168.1.50", "root")

		// THEN
		assert.True(t, ok)
	})

	t.Run("ユーザーが nil の場合は見つからない", func(t *testing.T) {
		// GIVEN
		a := &ACL{hashEntryCache: make(map[string][32]byte)}

		// WHEN
		_, ok := a.Lookup("127.0.0.1", "root")

		// THEN
		assert.False(t, ok)
	})

	t.Run("見つかったユーザーの AuthString が正しい", func(t *testing.T) {
		// GIVEN
		a := testACL(t, "secret", "%")

		// WHEN
		authString, ok := a.Lookup("127.0.0.1", "root")

		// THEN
		assert.True(t, ok)
		assert.True(t, VerifyCryptPassword("secret", authString))
	})

	t.Run("完全一致のホストでマッチする", func(t *testing.T) {
		// GIVEN
		a := testACL(t, "pass", "10.0.0.5")

		// WHEN
		_, ok := a.Lookup("10.0.0.5", "root")

		// THEN
		assert.True(t, ok)
	})

	t.Run("完全一致のホストで別 IP は見つからない", func(t *testing.T) {
		// GIVEN
		a := testACL(t, "pass", "10.0.0.5")

		// WHEN
		_, ok := a.Lookup("10.0.0.6", "root")

		// THEN
		assert.False(t, ok)
	})
}

func TestHashEntryCache(t *testing.T) {
	t.Run("Set/Get でキャッシュにエントリを追加・取得できる", func(t *testing.T) {
		// GIVEN
		a := &ACL{hashEntryCache: make(map[string][32]byte)}
		entry := [32]byte{1, 2, 3}

		// WHEN
		a.SetHashEntry("root", entry)
		got, ok := a.GetHashEntry("root")

		// THEN
		assert.True(t, ok)
		assert.Equal(t, entry, got)
	})

	t.Run("キャッシュにないユーザーは見つからない", func(t *testing.T) {
		// GIVEN
		a := &ACL{hashEntryCache: make(map[string][32]byte)}

		// WHEN
		_, ok := a.GetHashEntry("unknown")

		// THEN
		assert.False(t, ok)
	})

	t.Run("ClearHashEntry でキャッシュからエントリを削除できる", func(t *testing.T) {
		// GIVEN
		a := &ACL{hashEntryCache: make(map[string][32]byte)}
		a.SetHashEntry("root", [32]byte{1})

		// WHEN
		a.ClearHashEntry("root")

		// THEN
		_, ok := a.GetHashEntry("root")
		assert.False(t, ok)
	})
}

// testACL はテスト用の ACL を構築する
func testACL(t *testing.T, password, host string) *ACL {
	t.Helper()
	authString, err := CryptPassword(password)
	require.NoError(t, err)
	return NewACLFromCatalog("root", host, authString)
}

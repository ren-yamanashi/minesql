package acl

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestNewACL(t *testing.T) {
	t.Run("ユーザーから ACL を構築できる", func(t *testing.T) {
		// GIVEN
		user := NewUser("root", "pass", "%")

		// WHEN
		a := NewACL(user)

		// THEN
		found, ok := a.Lookup("127.0.0.1", "root")
		assert.True(t, ok)
		assert.Equal(t, "root", found.Username)
	})

	t.Run("nil ユーザーで ACL を構築できる", func(t *testing.T) {
		// WHEN
		a := NewACL(nil)

		// THEN
		_, ok := a.Lookup("127.0.0.1", "root")
		assert.False(t, ok)
	})
}

func TestNewACLFromCatalog(t *testing.T) {
	t.Run("カタログ情報から ACL を構築できる", func(t *testing.T) {
		// GIVEN
		authString := ComputeAuthString("mypass")

		// WHEN
		a := NewACLFromCatalog("admin", "192.168.1.%", authString)

		// THEN
		found, ok := a.Lookup("192.168.1.50", "admin")
		assert.True(t, ok)
		assert.Equal(t, "admin", found.Username)
		assert.Equal(t, "192.168.1.%", found.Host)
		assert.Equal(t, authString, found.AuthString)
	})

	t.Run("カタログから構築した ACL でホスト不一致は見つからない", func(t *testing.T) {
		// GIVEN
		a := NewACLFromCatalog("admin", "192.168.1.%", ComputeAuthString("pass"))

		// WHEN
		_, ok := a.Lookup("10.0.0.1", "admin")

		// THEN
		assert.False(t, ok)
	})

	t.Run("カタログから構築した ACL でユーザー名不一致は見つからない", func(t *testing.T) {
		// GIVEN
		a := NewACLFromCatalog("admin", "%", ComputeAuthString("pass"))

		// WHEN
		_, ok := a.Lookup("127.0.0.1", "root")

		// THEN
		assert.False(t, ok)
	})
}

func TestLookup(t *testing.T) {
	t.Run("ユーザー名とホストが一致する場合に見つかる", func(t *testing.T) {
		// GIVEN
		user := NewUser("root", "pass", "%")
		a := NewACL(user)

		// WHEN
		found, ok := a.Lookup("192.168.1.100", "root")

		// THEN
		assert.True(t, ok)
		assert.Equal(t, "root", found.Username)
	})

	t.Run("ユーザー名が一致しない場合は見つからない", func(t *testing.T) {
		// GIVEN
		user := NewUser("root", "pass", "%")
		a := NewACL(user)

		// WHEN
		_, ok := a.Lookup("192.168.1.100", "unknown")

		// THEN
		assert.False(t, ok)
	})

	t.Run("ホストが一致しない場合は見つからない", func(t *testing.T) {
		// GIVEN
		user := NewUser("root", "pass", "127.0.0.1")
		a := NewACL(user)

		// WHEN
		_, ok := a.Lookup("192.168.1.100", "root")

		// THEN
		assert.False(t, ok)
	})

	t.Run("サブネットパターンでホストがマッチする", func(t *testing.T) {
		// GIVEN
		user := NewUser("root", "pass", "192.168.1.%")
		a := NewACL(user)

		// WHEN
		found, ok := a.Lookup("192.168.1.50", "root")

		// THEN
		assert.True(t, ok)
		assert.Equal(t, "root", found.Username)
	})

	t.Run("ユーザーが nil の場合は見つからない", func(t *testing.T) {
		// GIVEN
		a := NewACL(nil)

		// WHEN
		_, ok := a.Lookup("127.0.0.1", "root")

		// THEN
		assert.False(t, ok)
	})

	t.Run("見つかったユーザーの AuthString が正しい", func(t *testing.T) {
		// GIVEN
		user := NewUser("root", "secret", "%")
		a := NewACL(user)

		// WHEN
		found, ok := a.Lookup("127.0.0.1", "root")

		// THEN
		assert.True(t, ok)
		stage1 := sha256.Sum256([]byte("secret"))
		expected := sha256.Sum256(stage1[:])
		assert.Equal(t, expected, found.AuthString)
	})

	t.Run("完全一致のホストでマッチする", func(t *testing.T) {
		// GIVEN
		user := NewUser("root", "pass", "10.0.0.5")
		a := NewACL(user)

		// WHEN
		found, ok := a.Lookup("10.0.0.5", "root")

		// THEN
		assert.True(t, ok)
		assert.Equal(t, "root", found.Username)
	})

	t.Run("完全一致のホストで別 IP は見つからない", func(t *testing.T) {
		// GIVEN
		user := NewUser("root", "pass", "10.0.0.5")
		a := NewACL(user)

		// WHEN
		_, ok := a.Lookup("10.0.0.6", "root")

		// THEN
		assert.False(t, ok)
	})
}

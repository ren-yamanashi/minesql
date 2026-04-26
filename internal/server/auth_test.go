package server

import (
	"crypto/sha256"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/acl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestACLWithCache は ACL を構築し、Hash Entry Cache にエントリを追加する
func setupTestACLWithCache(t *testing.T, host string) *acl.ACL {
	t.Helper()
	password := "root"
	authString, err := acl.CryptPassword(password)
	require.NoError(t, err)
	a := acl.NewACLFromCatalog("root", host, authString)

	// Fast Auth 用にキャッシュをセット
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])
	a.SetHashEntry("root", stage2)
	return a
}

// setupTestACLWithoutCache は ACL を構築するがキャッシュは空のまま
func setupTestACLWithoutCache(t *testing.T, host string) *acl.ACL {
	t.Helper()
	authString, err := acl.CryptPassword("root")
	require.NoError(t, err)
	return acl.NewACLFromCatalog("root", host, authString)
}

func TestAuthenticate(t *testing.T) {
	t.Run("キャッシュありで正しいパスワードなら認証成功", func(t *testing.T) {
		// GIVEN
		a := setupTestACLWithCache(t, "%")
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}
		scramble := computeClientScramble("root", nonce)

		// WHEN
		result, err := authenticate(a, "127.0.0.1", "root", scramble, nonce)

		// THEN
		assert.Equal(t, authSuccess, result)
		assert.NoError(t, err)
	})

	t.Run("不正なユーザー名で認証失敗", func(t *testing.T) {
		// GIVEN
		a := setupTestACLWithCache(t, "%")
		nonce := make([]byte, 20)
		scramble := computeClientScramble("root", nonce)

		// WHEN
		result, err := authenticate(a, "127.0.0.1", "unknown", scramble, nonce)

		// THEN
		assert.Equal(t, authFailed, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied for user 'unknown'")
	})

	t.Run("不正なパスワードで認証失敗", func(t *testing.T) {
		// GIVEN
		a := setupTestACLWithCache(t, "%")
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}
		scramble := computeClientScramble("wrong_password", nonce)

		// WHEN
		result, err := authenticate(a, "127.0.0.1", "root", scramble, nonce)

		// THEN
		assert.Equal(t, authFailed, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("キャッシュなしの場合は cacheMiss を返す", func(t *testing.T) {
		// GIVEN
		a := setupTestACLWithoutCache(t, "%")
		nonce := make([]byte, 20)
		scramble := computeClientScramble("root", nonce)

		// WHEN
		result, err := authenticate(a, "127.0.0.1", "root", scramble, nonce)

		// THEN
		assert.Equal(t, authCacheMiss, result)
		assert.NoError(t, err)
	})

	t.Run("異なる nonce で正しい scramble を計算すれば認証成功", func(t *testing.T) {
		// GIVEN
		a := setupTestACLWithCache(t, "%")
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 100)
		}
		scramble := computeClientScramble("root", nonce)

		// WHEN
		result, err := authenticate(a, "127.0.0.1", "root", scramble, nonce)

		// THEN
		assert.Equal(t, authSuccess, result)
		assert.NoError(t, err)
	})

	t.Run("scramble の長さが不正な場合に認証失敗", func(t *testing.T) {
		// GIVEN
		a := setupTestACLWithCache(t, "%")
		nonce := make([]byte, 20)
		shortScramble := make([]byte, 16)

		// WHEN
		result, err := authenticate(a, "127.0.0.1", "root", shortScramble, nonce)

		// THEN
		assert.Equal(t, authFailed, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("ホストが一致しない場合に認証失敗", func(t *testing.T) {
		// GIVEN: 127.0.0.1 のみ許可
		a := setupTestACLWithCache(t, "127.0.0.1")
		nonce := make([]byte, 20)
		scramble := computeClientScramble("root", nonce)

		// WHEN: 別のホストから接続
		result, err := authenticate(a, "192.168.1.100", "root", scramble, nonce)

		// THEN
		assert.Equal(t, authFailed, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("サブネットパターンでホストがマッチすれば認証成功", func(t *testing.T) {
		// GIVEN
		a := setupTestACLWithCache(t, "192.168.1.%")
		nonce := make([]byte, 20)
		scramble := computeClientScramble("root", nonce)

		// WHEN
		result, err := authenticate(a, "192.168.1.50", "root", scramble, nonce)

		// THEN
		assert.Equal(t, authSuccess, result)
		assert.NoError(t, err)
	})
}

func TestAuthMoreDataPacketBuild(t *testing.T) {
	t.Run("fast auth success パケットを構築できる", func(t *testing.T) {
		// GIVEN
		pkt := &authMoreDataPacket{statusByte: fastAuthSuccess}

		// WHEN
		buf := pkt.build()

		// THEN
		assert.Equal(t, byte(0x01), buf[0])
		assert.Equal(t, fastAuthSuccess, buf[1])
		assert.Len(t, buf, 2)
	})

	t.Run("packet interface を実装している", func(t *testing.T) {
		// GIVEN
		var p packet = &authMoreDataPacket{}

		// THEN
		assert.NotNil(t, p)
	})
}

// computeClientScramble はクライアント側の scramble 計算をシミュレートする
func computeClientScramble(password string, nonce []byte) []byte {
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])

	h := sha256.New()
	h.Write(stage2[:])
	h.Write(nonce)
	digest := h.Sum(nil)

	scramble := make([]byte, 32)
	for i := range scramble {
		scramble[i] = stage1[i] ^ digest[i]
	}
	return scramble
}

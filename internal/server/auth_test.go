package server

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"

	"minesql/internal/storage/acl"
)

func testACL() *acl.ACL {
	return acl.NewACL(acl.NewUser("root", "root", "%"))
}

func TestAuthenticate(t *testing.T) {
	t.Run("正しいユーザー名とパスワードで認証成功", func(t *testing.T) {
		// GIVEN
		a := testACL()
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}
		scramble := computeClientScramble("root", nonce)

		// WHEN
		err := authenticate(a, "127.0.0.1", "root", scramble, nonce)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("不正なユーザー名で認証失敗", func(t *testing.T) {
		// GIVEN
		a := testACL()
		nonce := make([]byte, 20)
		scramble := computeClientScramble("root", nonce)

		// WHEN
		err := authenticate(a, "127.0.0.1", "unknown", scramble, nonce)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied for user 'unknown'")
	})

	t.Run("不正なパスワードで認証失敗", func(t *testing.T) {
		// GIVEN
		a := testACL()
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}
		scramble := computeClientScramble("wrong_password", nonce)

		// WHEN
		err := authenticate(a, "127.0.0.1", "root", scramble, nonce)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("空パスワードで認証失敗 (パスワードは空ではない)", func(t *testing.T) {
		// GIVEN
		a := testACL()
		nonce := make([]byte, 20)

		// WHEN
		err := authenticate(a, "127.0.0.1", "root", []byte{}, nonce)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("異なる nonce で正しい scramble を計算すれば認証成功", func(t *testing.T) {
		// GIVEN
		a := testACL()
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 100)
		}
		scramble := computeClientScramble("root", nonce)

		// WHEN
		err := authenticate(a, "127.0.0.1", "root", scramble, nonce)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("scramble の長さが不正な場合に認証失敗", func(t *testing.T) {
		// GIVEN
		a := testACL()
		nonce := make([]byte, 20)
		shortScramble := make([]byte, 16)

		// WHEN
		err := authenticate(a, "127.0.0.1", "root", shortScramble, nonce)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("ホストが一致しない場合に認証失敗", func(t *testing.T) {
		// GIVEN: 127.0.0.1 のみ許可
		a := acl.NewACL(acl.NewUser("root", "root", "127.0.0.1"))
		nonce := make([]byte, 20)
		scramble := computeClientScramble("root", nonce)

		// WHEN: 別のホストから接続
		err := authenticate(a, "192.168.1.100", "root", scramble, nonce)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access denied")
	})

	t.Run("サブネットパターンでホストがマッチすれば認証成功", func(t *testing.T) {
		// GIVEN
		a := acl.NewACL(acl.NewUser("root", "root", "192.168.1.%"))
		nonce := make([]byte, 20)
		scramble := computeClientScramble("root", nonce)

		// WHEN
		err := authenticate(a, "192.168.1.50", "root", scramble, nonce)

		// THEN
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

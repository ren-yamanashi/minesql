package server

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuthenticate(t *testing.T) {
	t.Run("正しいユーザー名とパスワードで認証成功", func(t *testing.T) {
		// GIVEN: クライアント側の scramble 計算をシミュレート
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}
		scramble := computeClientScramble("root", nonce)

		// WHEN
		err := authenticate("root", scramble, nonce)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("不正なユーザー名で認証失敗", func(t *testing.T) {
		// GIVEN
		nonce := make([]byte, 20)
		scramble := computeClientScramble("root", nonce)

		// WHEN
		err := authenticate("unknown", scramble, nonce)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Access denied for user 'unknown'")
	})

	t.Run("不正なパスワードで認証失敗", func(t *testing.T) {
		// GIVEN: 異なるパスワードで scramble を計算
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}
		scramble := computeClientScramble("wrong_password", nonce)

		// WHEN
		err := authenticate("root", scramble, nonce)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Access denied")
	})

	t.Run("空パスワードで認証失敗 (固定パスワードは空ではない)", func(t *testing.T) {
		// GIVEN
		nonce := make([]byte, 20)

		// WHEN: 空の scramble (空パスワード)
		err := authenticate("root", []byte{}, nonce)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Access denied")
	})

	t.Run("異なる nonce で正しい scramble を計算すれば認証成功", func(t *testing.T) {
		// GIVEN: 別の nonce を使用
		nonce := make([]byte, 20)
		for i := range nonce {
			nonce[i] = byte(i + 100)
		}
		scramble := computeClientScramble("root", nonce)

		// WHEN
		err := authenticate("root", scramble, nonce)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("scramble の長さが不正な場合に認証失敗", func(t *testing.T) {
		// GIVEN: 16 バイトの scramble (正しくは 32 バイト)
		nonce := make([]byte, 20)
		shortScramble := make([]byte, 16)

		// WHEN
		err := authenticate("root", shortScramble, nonce)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Access denied")
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
//
// 1. stage1 = SHA256(password)
// 2. stage2 = SHA256(stage1)
// 3. digest = SHA256(stage2 || nonce)
// 4. scramble = XOR(stage1, digest)
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

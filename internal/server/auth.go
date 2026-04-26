package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"

	"minesql/internal/storage/acl"
)

// authenticate は caching_sha2_password の Fast Authentication を行う
//
// 認証成功時は nil を返す。失敗時はエラーを返す。
func authenticate(a *acl.ACL, host, username string, clientScramble []byte, nonce []byte) error {
	user, ok := a.Lookup(host, username)
	if !ok {
		return fmt.Errorf("access denied for user '%s'@'%s'", username, host)
	}

	// 空パスワードの判定
	if len(clientScramble) == 0 {
		emptyAuthString := acl.ComputeAuthString("")
		if user.AuthString == emptyAuthString {
			return nil
		}
		return fmt.Errorf("access denied for user '%s'@'%s'", username, host)
	}

	// サーバー側の検証:
	// 1. expected = SHA256(auth_string || nonce)
	h := sha256.New()
	h.Write(user.AuthString[:])
	h.Write(nonce)
	expected := h.Sum(nil)

	// 2. candidate_stage1 = XOR(client_scramble, expected)
	if len(clientScramble) != len(expected) {
		return fmt.Errorf("access denied for user '%s'@'%s'", username, host)
	}
	candidateStage1 := make([]byte, len(clientScramble))
	for i := range candidateStage1 {
		candidateStage1[i] = clientScramble[i] ^ expected[i]
	}

	// 3. candidate_stage2 = SHA256(candidate_stage1)
	candidateStage2 := sha256.Sum256(candidateStage1)

	// 4. candidate_stage2 == auth_string なら認証成功
	if subtle.ConstantTimeCompare(candidateStage2[:], user.AuthString[:]) != 1 {
		return fmt.Errorf("access denied for user '%s'@'%s'", username, host)
	}

	return nil
}

// authMoreDataPacket は AuthMoreData パケットを表す
//
// 構造
//   - 0x01 (AuthMoreData indicator)
//   - status_byte (0x03 = fast auth success, 0x04 = perform full auth)
type authMoreDataPacket struct {
	statusByte byte
}

const (
	fastAuthSuccess byte = 0x03
	performFullAuth byte = 0x04
)

// build は AuthMoreData パケットのペイロードを構築する
func (p *authMoreDataPacket) build() []byte {
	return []byte{0x01, p.statusByte}
}

package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
)

// 固定ユーザー
const (
	fixedUsername = "root"
	fixedPassword = "root"
)

// cachedHash は SHA256(SHA256(fixedPassword)) のキャッシュ
var cachedHash [32]byte

func init() {
	stage1 := sha256.Sum256([]byte(fixedPassword))
	cachedHash = sha256.Sum256(stage1[:])
}

// authenticate は caching_sha2_password の Fast Authentication を行う
//
// 認証成功時は nil を返す。失敗時はエラーを返す。
func authenticate(username string, clientScramble []byte, nonce []byte) error {
	if username != fixedUsername {
		return fmt.Errorf("Access denied for user '%s'", username)
	}

	// 空パスワードの判定
	if len(clientScramble) == 0 {
		if fixedPassword == "" {
			return nil
		}
		return fmt.Errorf("Access denied for user '%s'", username)
	}

	// サーバー側の検証:
	// 1. expected = SHA256(cached_hash || nonce)
	h := sha256.New()
	h.Write(cachedHash[:])
	h.Write(nonce)
	expected := h.Sum(nil)

	// 2. candidate_stage1 = XOR(client_scramble, expected)
	if len(clientScramble) != len(expected) {
		return fmt.Errorf("Access denied for user '%s'", username)
	}
	candidateStage1 := make([]byte, len(clientScramble))
	for i := range candidateStage1 {
		candidateStage1[i] = clientScramble[i] ^ expected[i]
	}

	// 3. candidate_stage2 = SHA256(candidate_stage1)
	candidateStage2 := sha256.Sum256(candidateStage1)

	// 4. candidate_stage2 == cached_hash なら認証成功
	if subtle.ConstantTimeCompare(candidateStage2[:], cachedHash[:]) != 1 {
		return fmt.Errorf("Access denied for user '%s'", username)
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

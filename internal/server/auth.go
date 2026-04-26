package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"

	"minesql/internal/storage/acl"
)

// authResult は認証結果を表す
type authResult int

const (
	authSuccess   authResult = iota // 認証成功 (Fast Auth)
	authCacheMiss                   // キャッシュミス (Complete Auth が必要)
	authFailed                      // 認証失敗
)

// authenticate は caching_sha2_password の Fast Authentication を試みる
//   - Hash Entry Cache にエントリがある場合は Fast Auth で検証する
//   - キャッシュにない場合は authCacheMiss を返す
func authenticate(a *acl.ACL, host, username string, clientScramble []byte, nonce []byte) (authResult, error) {
	_, ok := a.Lookup(host, username)
	if !ok {
		return authFailed, fmt.Errorf("access denied for user '%s'@'%s'", username, host)
	}

	// Hash Entry Cache から SHA256(SHA256(password)) を取得
	hashEntry, cached := a.GetHashEntry(username)
	if !cached {
		return authCacheMiss, nil
	}

	// 空パスワードの判定
	if len(clientScramble) == 0 {
		emptyHash := sha256.Sum256([]byte{})
		emptyDouble := sha256.Sum256(emptyHash[:])
		if subtle.ConstantTimeCompare(hashEntry[:], emptyDouble[:]) == 1 {
			return authSuccess, nil
		}
		return authFailed, fmt.Errorf("access denied for user '%s'@'%s'", username, host)
	}

	// サーバー側の検証 (Fast Auth)
	// 詳細は docs/architecture/account/authentication-plugin.md を参照
	expected := sha256.New()
	expected.Write(hashEntry[:])
	expected.Write(nonce)
	expectedDigest := expected.Sum(nil)

	if len(clientScramble) != len(expectedDigest) {
		return authFailed, fmt.Errorf("access denied for user '%s'@'%s'", username, host)
	}
	candidateStage1 := make([]byte, len(clientScramble))
	for i := range candidateStage1 {
		candidateStage1[i] = clientScramble[i] ^ expectedDigest[i]
	}
	candidateStage2 := sha256.Sum256(candidateStage1)
	if subtle.ConstantTimeCompare(candidateStage2[:], hashEntry[:]) != 1 {
		return authFailed, fmt.Errorf("access denied for user '%s'@'%s'", username, host)
	}

	return authSuccess, nil
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

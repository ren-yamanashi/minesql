package acl

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"strings"
)

// MySQL caching_sha2_password の $A$005$<salt><hash> 形式
//
// 参考: mysys/crypt_genhash_impl.cc, sql/auth/sha2_password.cc

const (
	cryptPrefix    = "$A$005$"
	cryptSaltLen   = 20
	cryptHashLen   = 43
	cryptRounds    = 5000
	cryptDigestLen = 32 // SHA-256
)

// b64t は MySQL の crypt_genhash_impl.cc で定義されている Base64 文字テーブル
var b64t = []byte("./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

// CryptPassword はパスワードからソルト付きハッシュ ($A$005$ 形式) を生成する
func CryptPassword(password string) (string, error) {
	salt, err := generateSalt(cryptSaltLen)
	if err != nil {
		return "", fmt.Errorf("failed to generate salt: %w", err)
	}
	hash := cryptHash([]byte(password), salt)
	return cryptPrefix + string(salt) + hash, nil
}

// VerifyCryptPassword は平文パスワードとソルト付きハッシュを照合する
func VerifyCryptPassword(password, crypted string) bool {
	// $A$005$ の後にソルト (20 バイト) + ハッシュ (43 バイト)
	if !strings.HasPrefix(crypted, cryptPrefix) {
		return false
	}
	rest := crypted[len(cryptPrefix):]
	if len(rest) != cryptSaltLen+cryptHashLen {
		return false
	}
	salt := []byte(rest[:cryptSaltLen])
	expectedHash := rest[cryptSaltLen:]

	actualHash := cryptHash([]byte(password), salt)
	return actualHash == expectedHash
}

// generateSalt は暗号論的に安全なソルトを生成する
// MySQL と同様に 7 ビット ASCII に制限し、NUL と '$' を避ける
func generateSalt(length int) ([]byte, error) {
	salt := make([]byte, length)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	for i := range salt {
		salt[i] &= 0x7F // 7 ビット ASCII
		if salt[i] == 0 || salt[i] == '$' {
			salt[i]++
		}
	}
	return salt, nil
}

// cryptHash は SHA-crypt アルゴリズム (SHA-256, 5000 回イテレーション) を実行する
//
// Ulrich Drepper の SHA-crypt 仕様に基づく
// 参考: http://people.redhat.com/drepper/SHA-crypt.txt
func cryptHash(password, salt []byte) string {
	// Alternate hash: SHA256(password + salt + password)
	altHash := sha256Sum(password, salt, password)

	// Initial hash: password + salt + altHash (password 長分) + ビットミキシング
	ctx := sha256.New()
	ctx.Write(password)
	ctx.Write(salt)

	// password の長さ分だけ alternate hash を追加
	plen := len(password)
	for remaining := plen; remaining > 0; remaining -= cryptDigestLen {
		if remaining > cryptDigestLen {
			ctx.Write(altHash)
		} else {
			ctx.Write(altHash[:remaining])
		}
	}

	// password の長さのビットに基づいて alternate hash / password を交互に追加
	for i := plen; i > 0; i >>= 1 {
		if i&1 != 0 {
			ctx.Write(altHash)
		} else {
			ctx.Write(password)
		}
	}
	digestA := ctx.Sum(nil)

	// P-bytes: SHA256(password を password 長分繰り返し) を password 長に切り詰め
	pCtx := sha256.New()
	for i := 0; i < plen; i++ {
		pCtx.Write(password)
	}
	pBytes := pCtx.Sum(nil)

	p := make([]byte, plen)
	for i := 0; i < plen; i++ {
		p[i] = pBytes[i%cryptDigestLen]
	}

	// S-bytes: SHA256(salt を (16 + digestA[0]) 回繰り返し) を salt 長に切り詰め
	sCtx := sha256.New()
	for i := 0; i < 16+int(digestA[0]); i++ {
		sCtx.Write(salt)
	}
	sBytes := sCtx.Sum(nil)

	s := make([]byte, len(salt))
	for i := 0; i < len(salt); i++ {
		s[i] = sBytes[i%cryptDigestLen]
	}

	// 5000 回のイテレーション
	dp := digestA
	for i := 0; i < cryptRounds; i++ {
		rCtx := sha256.New()
		if i&1 != 0 {
			rCtx.Write(p)
		} else {
			rCtx.Write(dp)
		}
		if i%3 != 0 {
			rCtx.Write(s)
		}
		if i%7 != 0 {
			rCtx.Write(p)
		}
		if i&1 != 0 {
			rCtx.Write(dp)
		} else {
			rCtx.Write(p)
		}
		dp = rCtx.Sum(nil)
	}

	// Step 21: 結果をエンコード
	return encodeCryptHash(dp)
}

// encodeCryptHash は 32 バイトのダイジェストを 43 文字の Base64 エンコードに変換する
//
// MySQL の crypt_genhash_impl.cc と同じバイト順序で処理する
func encodeCryptHash(dp []byte) string {
	var buf []byte
	buf = b64From24Bit(dp[0], dp[10], dp[20], 4, buf)
	buf = b64From24Bit(dp[21], dp[1], dp[11], 4, buf)
	buf = b64From24Bit(dp[12], dp[22], dp[2], 4, buf)
	buf = b64From24Bit(dp[3], dp[13], dp[23], 4, buf)
	buf = b64From24Bit(dp[24], dp[4], dp[14], 4, buf)
	buf = b64From24Bit(dp[15], dp[25], dp[5], 4, buf)
	buf = b64From24Bit(dp[6], dp[16], dp[26], 4, buf)
	buf = b64From24Bit(dp[27], dp[7], dp[17], 4, buf)
	buf = b64From24Bit(dp[18], dp[28], dp[8], 4, buf)
	buf = b64From24Bit(dp[9], dp[19], dp[29], 4, buf)
	buf = b64From24Bit(0, dp[31], dp[30], 3, buf)
	return string(buf)
}

// b64From24Bit は 3 バイト (24 ビット) を Base64 文字に変換する
func b64From24Bit(b2, b1, b0 byte, n int, buf []byte) []byte {
	w := uint32(b2)<<16 | uint32(b1)<<8 | uint32(b0)
	for i := 0; i < n; i++ {
		buf = append(buf, b64t[w&0x3F])
		w >>= 6
	}
	return buf
}

func sha256Sum(parts ...[]byte) []byte {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

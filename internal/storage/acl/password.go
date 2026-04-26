package acl

import (
	"crypto/rand"
	"encoding/base64"
)

// GeneratePassword はランダムパスワードを生成する
func GeneratePassword() (string, error) {
	buf := make([]byte, 12) // 12 bytes → base64 で 16 文字
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(buf), nil
}

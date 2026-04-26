package acl

import "crypto/sha256"

// User はユーザーアカウントの認証情報を表す
type User struct {
	Username   string
	Host       string
	AuthString [32]byte // SHA256(SHA256(password))
}

// NewUser はパスワードからハッシュを計算してユーザーを生成する
func NewUser(username, password, host string) *User {
	return &User{
		Username:   username,
		Host:       host,
		AuthString: ComputeAuthString(password),
	}
}

// ComputeAuthString はパスワードから authentication_string (SHA256(SHA256(password))) を計算する
func ComputeAuthString(password string) [32]byte {
	stage1 := sha256.Sum256([]byte(password))
	return sha256.Sum256(stage1[:])
}

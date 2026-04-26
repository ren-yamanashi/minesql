package acl

import "strings"

// ACL はアクセス制御リストを表す
type ACL struct {
	user *User
}

// NewACL はユーザーから ACL を構築する
func NewACL(user *User) *ACL {
	return &ACL{user: user}
}

// NewACLFromCatalog はカタログのユーザーメタデータから ACL を構築する
func NewACLFromCatalog(username, host string, authString [32]byte) *ACL {
	return &ACL{user: &User{
		Username:   username,
		Host:       host,
		AuthString: authString,
	}}
}

// Lookup はホスト名とユーザー名でユーザーを検索する
//
// ホストマッチングの優先順位: 完全一致 → サブネットパターン → % (全許可)
func (a *ACL) Lookup(host, username string) (*User, bool) {
	if a.user == nil {
		return nil, false
	}
	if a.user.Username != username {
		return nil, false
	}
	if !MatchHost(a.user.Host, host) {
		return nil, false
	}
	return a.user, true
}

// MatchHost はホストパターンが接続元ホストにマッチするか判定する
//   - 完全一致: "192.168.1.100" は "192.168.1.100" にのみマッチ
//   - サブネットパターン: "192.168.1.%" は "192.168.1." で始まる全ホストにマッチ
//   - 全許可: "%" は全ホストにマッチ
func MatchHost(pattern, host string) bool {
	if pattern == "%" {
		return true
	}
	if !strings.Contains(pattern, "%") {
		return pattern == host
	}
	// サブネットパターン: % の前の部分がプレフィックスとして一致するか
	prefix := strings.TrimSuffix(pattern, "%")
	return strings.HasPrefix(host, prefix)
}

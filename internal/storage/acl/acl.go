package acl

import "strings"

// user はユーザーアカウントの認証情報を表す
type user struct {
	username   string
	host       string
	authString string // ソルト付きハッシュ ($A$005$ 形式)
}

// ACL はアクセス制御リストを表す
type ACL struct {
	u              *user
	hashEntryCache map[string][32]byte // username → SHA256(SHA256(password))
}

// NewACLFromCatalog はカタログのユーザーメタデータから ACL を構築する
func NewACLFromCatalog(username, host string, authString string) *ACL {
	return &ACL{
		u: &user{
			username:   username,
			host:       host,
			authString: authString,
		},
		hashEntryCache: make(map[string][32]byte),
	}
}

// SetHashEntry は Hash Entry Cache にエントリを追加する
func (a *ACL) SetHashEntry(username string, entry [32]byte) {
	a.hashEntryCache[username] = entry
}

// GetHashEntry は Hash Entry Cache からエントリを取得する
func (a *ACL) GetHashEntry(username string) ([32]byte, bool) {
	entry, ok := a.hashEntryCache[username]
	return entry, ok
}

// ClearHashEntry は Hash Entry Cache からエントリを削除する
func (a *ACL) ClearHashEntry(username string) {
	delete(a.hashEntryCache, username)
}

// Lookup はホスト名とユーザー名でユーザーを検索し、authentication_string を返す
//
// ホストマッチングの優先順位: 完全一致 → サブネットパターン → % (全許可)
func (a *ACL) Lookup(host, username string) (authString string, ok bool) {
	if a.u == nil {
		return "", false
	}
	if a.u.username != username {
		return "", false
	}
	if !MatchHost(a.u.host, host) {
		return "", false
	}
	return a.u.authString, true
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

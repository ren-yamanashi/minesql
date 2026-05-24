package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type UserRecord struct {
	username   string // ユーザー名
	host       string // ホスト名
	authString []byte // 認証文字列
}

func NewUserRecord(username, host string, authString []byte) UserRecord {
	return UserRecord{
		username:   username,
		host:       host,
		authString: authString,
	}
}

func (ur UserRecord) Username() string   { return ur.username }
func (ur UserRecord) Host() string       { return ur.host }
func (ur UserRecord) AuthString() []byte { return ur.authString }

func (ur UserRecord) Encode() btree.Record {
	// key = username
	var key []byte
	encode.Encode([][]byte{[]byte(ur.username)}, &key)

	// nonKey = host + authString
	var nonKey []byte
	encode.Encode([][]byte{[]byte(ur.host), ur.authString}, &nonKey)

	return btree.NewRecord(nil, key, nonKey)
}

func DecodeUserRecord(record btree.Record) UserRecord {
	// key = [username]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	username := string(key[0])

	// nonKey = [host, authString]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	host := string(nonKey[0])
	authString := nonKey[1]

	return NewUserRecord(username, host, authString)
}

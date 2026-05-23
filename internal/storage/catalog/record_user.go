package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type UserRecord struct {
	Username   string // ユーザー名
	Host       string // ホスト名
	AuthString []byte // 認証文字列
}

func newUserRecord(username, host string, authString []byte) UserRecord {
	return UserRecord{
		Username:   username,
		Host:       host,
		AuthString: authString,
	}
}

// encode は btree.Record にエンコードする
func (ur UserRecord) encode() btree.Record {
	// key = username
	var key []byte
	encode.Encode([][]byte{[]byte(ur.Username)}, &key)

	// nonKey = host + authString
	var nonKey []byte
	encode.Encode([][]byte{[]byte(ur.Host), ur.AuthString}, &nonKey)

	return btree.NewRecord(nil, key, nonKey)
}

// decodeUserRecord は btree.Record から userRecord にデコードする
func decodeUserRecord(record btree.Record) UserRecord {
	// key = [username]
	var key [][]byte
	encode.Decode(record.Key(), &key)
	username := string(key[0])

	// nonKey = [host, authString]
	var nonKey [][]byte
	encode.Decode(record.NonKey(), &nonKey)
	host := string(nonKey[0])
	authString := nonKey[1]

	return newUserRecord(username, host, authString)
}

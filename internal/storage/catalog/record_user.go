package catalog

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type userRecord struct {
	username   string // ユーザー名
	host       string // ホスト名
	authString []byte // 認証文字列
}

func newUserRecord(username, host string, authString []byte) userRecord {
	return userRecord{
		username:   username,
		host:       host,
		authString: authString,
	}
}

// encode は node.Record にエンコードする
func (ur userRecord) encode() node.Record {
	// key = username
	var key []byte
	encode.Encode([][]byte{[]byte(ur.username)}, &key)

	// nonKey = host + authString
	var nonKey []byte
	encode.Encode([][]byte{[]byte(ur.host), ur.authString}, &nonKey)

	return node.NewRecord(nil, key, nonKey)
}

// decodeUserRecord は node.Record から userRecord にデコードする
func decodeUserRecord(record node.Record) userRecord {
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

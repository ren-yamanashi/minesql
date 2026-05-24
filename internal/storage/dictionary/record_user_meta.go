package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
)

type UserMetaRecord struct {
	username   string // ユーザー名
	host       string // ホスト名
	authString []byte // 認証文字列
}

func NewUserMetaRecord(username, host string, authString []byte) UserMetaRecord {
	return UserMetaRecord{
		username:   username,
		host:       host,
		authString: authString,
	}
}

func (ur UserMetaRecord) Username() string   { return ur.username }
func (ur UserMetaRecord) Host() string       { return ur.host }
func (ur UserMetaRecord) AuthString() []byte { return ur.authString }

func (ur UserMetaRecord) Encode() btree.Record {
	// key = username
	key := encode.Encode(nil, [][]byte{[]byte(ur.username)})

	// nonKey = host + authString
	nonKey := encode.Encode(nil, [][]byte{[]byte(ur.host), ur.authString})

	return btree.NewRecord(nil, key, nonKey)
}

func DecodeUserMetaRecord(record btree.Record) (UserMetaRecord, error) {
	// key = [username]
	key, err := encode.Decode(record.Key())
	if err != nil {
		return UserMetaRecord{}, err
	}
	username := string(key[0])

	// nonKey = [host, authString]
	nonKey, err := encode.Decode(record.NonKey())
	if err != nil {
		return UserMetaRecord{}, err
	}
	host := string(nonKey[0])
	authString := nonKey[1]

	return NewUserMetaRecord(username, host, authString), nil
}

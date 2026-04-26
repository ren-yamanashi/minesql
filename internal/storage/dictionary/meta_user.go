package dictionary

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/page"
)

// UserMeta はユーザーアカウントのメタデータを表す
type UserMeta struct {
	MetaPageId page.PageId // ユーザーメタデータが格納される B+Tree のメタページ ID
	Username   string
	Host       string
	AuthString [32]byte // SHA256(SHA256(password))
}

// Insert はユーザーメタデータを B+Tree に挿入する
func (um *UserMeta) Insert(bp *buffer.BufferPool) error {
	btr := btree.NewBTree(um.MetaPageId)

	// キー: Username
	var encodedKey []byte
	encode.Encode([][]byte{[]byte(um.Username)}, &encodedKey)

	// 非キー: Host, AuthString
	var encodedNonKey []byte
	encode.Encode([][]byte{[]byte(um.Host), um.AuthString[:]}, &encodedNonKey)

	return btr.Insert(bp, node.NewRecord(nil, encodedKey, encodedNonKey))
}

// Update はユーザーメタデータを B+Tree 上で更新する
func (um *UserMeta) Update(bp *buffer.BufferPool) error {
	btr := btree.NewBTree(um.MetaPageId)

	// キー: Username
	var encodedKey []byte
	encode.Encode([][]byte{[]byte(um.Username)}, &encodedKey)

	// 非キー: Host, AuthString
	var encodedNonKey []byte
	encode.Encode([][]byte{[]byte(um.Host), um.AuthString[:]}, &encodedNonKey)

	return btr.Update(bp, node.NewRecord(nil, encodedKey, encodedNonKey))
}

// loadUserMeta はユーザーメタデータを B+Tree から読み込む
func loadUserMeta(bp *buffer.BufferPool, metaPageId page.PageId) ([]*UserMeta, error) {
	userMetaTree := btree.NewBTree(metaPageId)
	iter, err := userMetaTree.Search(bp, btree.SearchModeStart{})
	if err != nil {
		return nil, err
	}

	var users []*UserMeta
	for {
		record, ok := iter.Get()
		if !ok {
			break
		}

		// キーをデコード (Username)
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		username := string(keyParts[0])

		// 非キーをデコード (Host, AuthString)
		var nonKeyParts [][]byte
		encode.Decode(record.NonKeyBytes(), &nonKeyParts)
		host := string(nonKeyParts[0])

		var authString [32]byte
		copy(authString[:], nonKeyParts[1])

		users = append(users, &UserMeta{
			MetaPageId: metaPageId,
			Username:   username,
			Host:       host,
			AuthString: authString,
		})

		if err := iter.Advance(bp); err != nil {
			return nil, err
		}
	}

	return users, nil
}

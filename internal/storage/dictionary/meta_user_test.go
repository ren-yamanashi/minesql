package dictionary

import (
	"crypto/sha256"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserMeta_Insert(t *testing.T) {
	t.Run("ユーザーメタデータを B+Tree に挿入できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		authString := computeTestAuthString("mypassword")
		userMeta := &UserMeta{
			MetaPageId: metaPageId,
			Username:   "root",
			Host:       "%",
			AuthString: authString,
		}

		// WHEN
		err = userMeta.Insert(bp)

		// THEN: B+Tree から挿入したデータを検索して確認
		assert.NoError(t, err)

		btr := btree.NewBTree(metaPageId)
		iter, err := btr.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)

		// key (Username) をデコード
		var keyParts [][]byte
		encode.Decode(record.KeyBytes(), &keyParts)
		assert.Equal(t, "root", string(keyParts[0]))

		// value (Host, AuthString) をデコード
		var valueParts [][]byte
		encode.Decode(record.NonKeyBytes(), &valueParts)
		assert.Equal(t, "%", string(valueParts[0]))
		assert.Equal(t, authString[:], valueParts[1])
	})

	t.Run("AuthString の 32 バイトが正しく保存される", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		// 全バイトが異なる AuthString
		var authString [32]byte
		for i := range authString {
			authString[i] = byte(i)
		}

		userMeta := &UserMeta{
			MetaPageId: metaPageId,
			Username:   "testuser",
			Host:       "127.0.0.1",
			AuthString: authString,
		}

		// WHEN
		err = userMeta.Insert(bp)
		assert.NoError(t, err)

		// THEN
		btr := btree.NewBTree(metaPageId)
		iter, err := btr.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)

		var valueParts [][]byte
		encode.Decode(record.NonKeyBytes(), &valueParts)
		assert.Equal(t, authString[:], valueParts[1])
	})
}

func TestLoadUserMeta(t *testing.T) {
	t.Run("ユーザーメタデータを読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		authString := computeTestAuthString("secret")
		userMeta := &UserMeta{
			MetaPageId: cat.UserMetaPageId,
			Username:   "admin",
			Host:       "192.168.1.%",
			AuthString: authString,
		}
		err = userMeta.Insert(bp)
		assert.NoError(t, err)

		// WHEN
		users, err := loadUserMeta(bp, cat.UserMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(users))
		assert.Equal(t, "admin", users[0].Username)
		assert.Equal(t, "192.168.1.%", users[0].Host)
		assert.Equal(t, authString, users[0].AuthString)
	})

	t.Run("該当するユーザーがない場合は空のスライスを返す", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		// WHEN
		users, err := loadUserMeta(bp, cat.UserMetaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, users)
	})
}

func computeTestAuthString(password string) [32]byte {
	stage1 := sha256.Sum256([]byte(password))
	return sha256.Sum256(stage1[:])
}

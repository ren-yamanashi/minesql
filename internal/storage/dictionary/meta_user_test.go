package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/acl"
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

		authString := cryptTestAuthString(t, "mypassword")
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
		assert.Equal(t, []byte(authString), valueParts[1])
	})

	t.Run("AuthString の可変長バイト列が正しく保存される", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		authString := cryptTestAuthString(t, "testpassword")

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
		assert.Equal(t, []byte(authString), valueParts[1])
	})
}

func TestUserMeta_Update(t *testing.T) {
	t.Run("ユーザーメタデータの AuthString を更新できる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		oldAuthString := cryptTestAuthString(t, "oldpassword")
		userMeta := &UserMeta{
			MetaPageId: metaPageId,
			Username:   "root",
			Host:       "%",
			AuthString: oldAuthString,
		}
		err = userMeta.Insert(bp)
		assert.NoError(t, err)

		// WHEN: AuthString を新しい値に更新
		newAuthString := cryptTestAuthString(t, "newpassword")
		userMeta.AuthString = newAuthString
		err = userMeta.Update(bp)

		// THEN
		assert.NoError(t, err)

		// B+Tree から読み込んで更新を確認
		users, err := loadUserMeta(bp, metaPageId)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(users))
		assert.Equal(t, "root", users[0].Username)
		assert.Equal(t, "%", users[0].Host)
		assert.Equal(t, newAuthString, users[0].AuthString)
	})

	t.Run("存在しないユーザーの更新はエラーになる", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		metaPageId, err := bp.AllocatePageId(page.FileId(0))
		assert.NoError(t, err)
		_, err = btree.CreateBTree(bp, metaPageId)
		assert.NoError(t, err)

		userMeta := &UserMeta{
			MetaPageId: metaPageId,
			Username:   "nonexistent",
			Host:       "%",
			AuthString: cryptTestAuthString(t, "pass"),
		}

		// WHEN
		err = userMeta.Update(bp)

		// THEN
		assert.Error(t, err)
	})
}

func TestLoadUserMeta(t *testing.T) {
	t.Run("ユーザーメタデータを読み込める", func(t *testing.T) {
		// GIVEN
		bp, tmpdir := InitCatalogDisk(t)
		defer removeTmpdir(t, tmpdir)

		cat, err := CreateCatalog(bp)
		assert.NoError(t, err)

		authString := cryptTestAuthString(t, "secret")
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

func cryptTestAuthString(t *testing.T, password string) string {
	t.Helper()
	s, err := acl.CryptPassword(password)
	require.NoError(t, err)
	return s
}

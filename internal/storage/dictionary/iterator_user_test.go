package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/stretchr/testify/assert"
)

func TestUserIteratorClose(t *testing.T) {
	t.Run("検索結果のイテレータを Close できる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		ct, err := CreateCatalog(bp)
		assert.NoError(t, err)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		err = ct.UserMeta().Insert(mtr, NewUserMetaRecord("testuser", "%", []byte("authstring")))
		assert.NoError(t, err)

		iter, err := ct.UserMeta().Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		// THEN
		_, _, _ = iter.Next()
		iter.Close()

	})

	t.Run("イテレーション前に Close できる", func(t *testing.T) {
		// GIVEN
		bp := setupCatalogTestBufferPool(t)
		ct, err := CreateCatalog(bp)
		assert.NoError(t, err)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()

		iter, err := ct.UserMeta().Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		// WHEN
		// THEN
		iter.Close()
	})
}

func TestUserIteratorNext(t *testing.T) {
	t.Run("レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		um, bp := setupTestUserMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = um.Insert(mtr, NewUserMetaRecord("alice", "localhost", []byte("auth1")))
		_ = um.Insert(mtr, NewUserMetaRecord("bob", "%", []byte("auth2")))
		iter, err := um.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()

		// WHEN
		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "alice", r1.Username())
		assert.Equal(t, "localhost", r1.Host())
		assert.Equal(t, []byte("auth1"), r1.AuthString())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "bob", r2.Username())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータの場合は false を返す", func(t *testing.T) {
		// GIVEN
		um, bp := setupTestUserMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		iter, err := um.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		defer iter.Close()

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

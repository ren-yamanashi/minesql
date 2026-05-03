package dictionary

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestUserMetaInsert(t *testing.T) {
	t.Run("ユーザーを挿入できる", func(t *testing.T) {
		// GIVEN
		um := setupTestUserMeta(t)

		// WHEN
		err := um.Insert("alice", "localhost", []byte("auth123"))

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じユーザー名を重複挿入すると ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		um := setupTestUserMeta(t)
		_ = um.Insert("alice", "localhost", []byte("auth1"))

		// WHEN
		err := um.Insert("alice", "%", []byte("auth2"))

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})
}

func TestUserMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		um := setupTestUserMeta(t)
		_ = um.Insert("alice", "localhost", []byte("auth1"))
		_ = um.Insert("bob", "%", []byte("auth2"))

		// WHEN
		iter, err := um.Search(SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "alice", r1.username)
		assert.Equal(t, "localhost", r1.host)
		assert.Equal(t, []byte("auth1"), r1.authString)

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "bob", r2.username)

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("SearchModeKey で指定したユーザーを検索できる", func(t *testing.T) {
		// GIVEN
		um := setupTestUserMeta(t)
		_ = um.Insert("alice", "localhost", []byte("auth1"))
		_ = um.Insert("bob", "%", []byte("auth2"))

		// WHEN
		iter, err := um.Search(SearchModeKey{Key: [][]byte{[]byte("bob")}})
		assert.NoError(t, err)

		r, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "bob", r.username)
		assert.Equal(t, "%", r.host)
		assert.Equal(t, []byte("auth2"), r.authString)
	})

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		um := setupTestUserMeta(t)

		// WHEN
		iter, err := um.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// setupTestUserMeta はテスト用の UserMeta を作成する
func setupTestUserMeta(t *testing.T) *UserMeta {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	tree, err := btree.CreateBtree(bp, page.FileId(0))
	if err != nil {
		t.Fatalf("B+Tree の作成に失敗: %v", err)
	}
	return &UserMeta{tree: tree}
}

// setupDictTestBufferPool は dictionary テスト用のバッファプールを作成する
func setupDictTestBufferPool(t *testing.T) *buffer.BufferPool {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dict_test.db")
	fileId := page.FileId(0)
	hf, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })
	bp := buffer.NewBufferPool(page.PageSize * 10)
	bp.RegisterHeapFile(fileId, hf)
	return bp
}

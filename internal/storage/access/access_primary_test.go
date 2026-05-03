package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreatePrimaryIndex(t *testing.T) {
	t.Run("空のプライマリインデックスを作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)

		// WHEN
		pi, err := CreatePrimaryIndex(bp, page.FileId(0))

		// THEN
		assert.NoError(t, err)
		count, err := pi.LeafPageCount()
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestPrimaryIndexInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)

		// WHEN
		err := pi.Insert([][]byte{[]byte("pk1"), []byte("val1")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じキーの重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("val1")})

		// WHEN
		err := pi.Insert([][]byte{[]byte("pk1"), []byte("val2")})

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("複合プライマリキーのレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 2)

		// WHEN
		err := pi.Insert([][]byte{[]byte("pk1"), []byte("pk2"), []byte("val1")})

		// THEN
		assert.NoError(t, err)
		iter, _ := pi.Search(SearchModeKey{Key: [][]byte{[]byte("pk1"), []byte("pk2")}})
		r, ok, _ := iter.Next()
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("pk1"), []byte("pk2"), []byte("val1")}, r)
	})

	t.Run("複合プライマリキーで PK の一部が異なれば別レコードとして挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 2)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("pk2"), []byte("val1")})

		// WHEN
		err := pi.Insert([][]byte{[]byte("pk1"), []byte("pk3"), []byte("val2")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("論理削除済みの同一キーが存在する場合は上書きできる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("val1")})
		_ = pi.SoftDelete([][]byte{[]byte("pk1"), []byte("val1")})

		// WHEN
		err := pi.Insert([][]byte{[]byte("pk1"), []byte("val2")})

		// THEN
		assert.NoError(t, err)
	})
}

func TestPrimaryIndexSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("val1")})
		_ = pi.Insert([][]byte{[]byte("pk2"), []byte("val2")})

		// WHEN
		iter, err := pi.Search(SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, _ := iter.Next()
		r2, ok2, _ := iter.Next()
		_, ok3, _ := iter.Next()

		// THEN
		assert.True(t, ok1)
		assert.Equal(t, [][]byte{[]byte("pk1"), []byte("val1")}, r1)
		assert.True(t, ok2)
		assert.Equal(t, [][]byte{[]byte("pk2"), []byte("val2")}, r2)
		assert.False(t, ok3)
	})

	t.Run("SearchModeKey で指定したキーから検索できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("val1")})
		_ = pi.Insert([][]byte{[]byte("pk2"), []byte("val2")})

		// WHEN
		iter, err := pi.Search(SearchModeKey{Key: [][]byte{[]byte("pk2")}})
		assert.NoError(t, err)

		r1, ok1, _ := iter.Next()

		// THEN
		assert.True(t, ok1)
		assert.Equal(t, [][]byte{[]byte("pk2"), []byte("val2")}, r1)
	})

	t.Run("空のインデックスを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)

		// WHEN
		iter, err := pi.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, _ := iter.Next()

		// THEN
		assert.False(t, ok)
	})
}

func TestPrimaryIndexSoftDelete(t *testing.T) {
	t.Run("レコードを論理削除できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("val1")})

		// WHEN
		err := pi.SoftDelete([][]byte{[]byte("pk1"), []byte("val1")})

		// THEN
		assert.NoError(t, err)
		// 論理削除後は検索で見えなくなる
		iter, _ := pi.Search(SearchModeStart{})
		_, ok, _ := iter.Next()
		assert.False(t, ok)
	})

	t.Run("存在しないキーを論理削除すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)

		// WHEN
		err := pi.SoftDelete([][]byte{[]byte("pk1"), []byte("val1")})

		// THEN
		assert.ErrorIs(t, err, btree.ErrKeyNotFound)
	})

	t.Run("既に論理削除済みのレコードを論理削除すると ErrAlreadyDeleted を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("val1")})
		_ = pi.SoftDelete([][]byte{[]byte("pk1"), []byte("val1")})

		// WHEN
		err := pi.SoftDelete([][]byte{[]byte("pk1"), []byte("val1")})

		// THEN
		assert.ErrorIs(t, err, ErrAlreadyDeleted)
	})
}

func TestPrimaryIndexUpdateInplace(t *testing.T) {
	t.Run("レコードを更新できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("val1")})

		// WHEN
		err := pi.UpdateInplace(
			[][]byte{[]byte("pk1"), []byte("val1")},
			[][]byte{[]byte("pk1"), []byte("val2")},
		)

		// THEN
		assert.NoError(t, err)
		iter, _ := pi.Search(SearchModeKey{Key: [][]byte{[]byte("pk1")}})
		r, ok, _ := iter.Next()
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("pk1"), []byte("val2")}, r)
	})

	t.Run("複合プライマリキーのレコードを更新できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 2)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("pk2"), []byte("val1")})

		// WHEN
		err := pi.UpdateInplace(
			[][]byte{[]byte("pk1"), []byte("pk2"), []byte("val1")},
			[][]byte{[]byte("pk1"), []byte("pk2"), []byte("val2")},
		)

		// THEN
		assert.NoError(t, err)
		iter, _ := pi.Search(SearchModeKey{Key: [][]byte{[]byte("pk1"), []byte("pk2")}})
		r, ok, _ := iter.Next()
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("pk1"), []byte("pk2"), []byte("val2")}, r)
	})

	t.Run("存在しないキーを更新すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)

		// WHEN
		err := pi.UpdateInplace(
			[][]byte{[]byte("pk1"), []byte("val1")},
			[][]byte{[]byte("pk1"), []byte("val2")},
		)

		// THEN
		assert.ErrorIs(t, err, btree.ErrKeyNotFound)
	})
}

func TestPrimaryIndexDelete(t *testing.T) {
	t.Run("レコードを物理削除できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("val1")})
		pr := newPrimaryRecord(1, 0, [][]byte{[]byte("pk1"), []byte("val1")})

		// WHEN
		err := pi.delete(pr)

		// THEN
		assert.NoError(t, err)
		iter, _ := pi.Search(SearchModeStart{})
		_, ok, _ := iter.Next()
		assert.False(t, ok)
	})

	t.Run("存在しないキーを物理削除すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)
		pr := newPrimaryRecord(1, 0, [][]byte{[]byte("pk1"), []byte("val1")})

		// WHEN
		err := pi.delete(pr)

		// THEN
		assert.ErrorIs(t, err, btree.ErrKeyNotFound)
	})

	t.Run("複合プライマリキーのレコードを物理削除できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 2)
		_ = pi.Insert([][]byte{[]byte("pk1"), []byte("pk2"), []byte("val1")})
		pr := newPrimaryRecord(2, 0, [][]byte{[]byte("pk1"), []byte("pk2"), []byte("val1")})

		// WHEN
		err := pi.delete(pr)

		// THEN
		assert.NoError(t, err)
		iter, _ := pi.Search(SearchModeStart{})
		_, ok, _ := iter.Next()
		assert.False(t, ok)
	})
}

func TestPrimaryIndexLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)

		// WHEN
		count, err := pi.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestPrimaryIndexHeight(t *testing.T) {
	t.Run("ツリーの高さを取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t, 1)

		// WHEN
		height, err := pi.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// setupTestPrimaryIndex はテスト用のプライマリインデックスを作成する
func setupTestPrimaryIndex(t *testing.T, pkCount int) *PrimaryIndex {
	t.Helper()
	bp := setupSecondaryTestBufferPool(t)
	pi, err := CreatePrimaryIndex(bp, page.FileId(0))
	if err != nil {
		t.Fatalf("PrimaryIndex の作成に失敗: %v", err)
	}
	pi.SetPkCount(pkCount)
	return pi
}

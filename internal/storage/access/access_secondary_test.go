package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreateSecondaryIndex(t *testing.T) {
	t.Run("空のセカンダリインデックスを作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)

		// WHEN
		si, err := createTestSecondaryIndex(t, bp, 1, false)

		// THEN
		assert.NoError(t, err)
		count, err := si.LeafPageCount()
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestSecondaryIndexInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)

		// WHEN
		err := si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じ SK でも PK が異なれば挿入できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		err := si.Insert([][]byte{[]byte("sk1"), []byte("pk2")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じ SK+PK の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		err := si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("論理削除済みの同一キーが存在する場合は上書きできる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})
		_ = si.SoftDelete([][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		err := si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("unique index の場合に同じ SK の active レコードがあると ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, true)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		err := si.Insert([][]byte{[]byte("sk1"), []byte("pk2")})

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("複合セカンダリキーのレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 2, false)

		// WHEN
		err := si.Insert([][]byte{[]byte("sk1"), []byte("sk2"), []byte("pk1")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("複合セカンダリキーで SK の一部が異なれば別レコードとして挿入できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 2, false)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("sk2"), []byte("pk1")})

		// WHEN
		err := si.Insert([][]byte{[]byte("sk1"), []byte("sk3"), []byte("pk1")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("unique index で複合セカンダリキーが完全一致する場合は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 2, true)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("sk2"), []byte("pk1")})

		// WHEN
		err := si.Insert([][]byte{[]byte("sk1"), []byte("sk2"), []byte("pk2")})

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("unique index でも論理削除済みの SK なら挿入できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, true)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})
		_ = si.SoftDelete([][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		err := si.Insert([][]byte{[]byte("sk1"), []byte("pk2")})

		// THEN
		assert.NoError(t, err)
	})
}

func TestSecondaryIndexDelete(t *testing.T) {
	t.Run("レコードを物理削除できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		err := si.Delete([][]byte{[]byte("sk1"), []byte("pk1")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("存在しないキーを削除すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)

		// WHEN
		err := si.Delete([][]byte{[]byte("sk1"), []byte("pk1")})

		// THEN
		assert.ErrorIs(t, err, btree.ErrKeyNotFound)
	})
}

func TestSecondaryIndexSoftDelete(t *testing.T) {
	t.Run("レコードを論理削除できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		err := si.SoftDelete([][]byte{[]byte("sk1"), []byte("pk1")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("存在しないキーを論理削除すると ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)

		// WHEN
		err := si.SoftDelete([][]byte{[]byte("sk1"), []byte("pk1")})

		// THEN
		assert.ErrorIs(t, err, btree.ErrKeyNotFound)
	})

	t.Run("既に論理削除済みのレコードを論理削除すると ErrAlreadyDeleted を返す", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)
		_ = si.Insert([][]byte{[]byte("sk1"), []byte("pk1")})
		_ = si.SoftDelete([][]byte{[]byte("sk1"), []byte("pk1")})

		// WHEN
		err := si.SoftDelete([][]byte{[]byte("sk1"), []byte("pk1")})

		// THEN
		assert.ErrorIs(t, err, ErrAlreadyDeleted)
	})
}

func TestSecondaryIndexLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)

		// WHEN
		count, err := si.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestSecondaryIndexHeight(t *testing.T) {
	t.Run("ツリーの高さを取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupSecondaryTestBufferPool(t)
		si, _ := createTestSecondaryIndex(t, bp, 1, false)

		// WHEN
		height, err := si.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// createTestSecondaryIndex はテスト用のセカンダリインデックスを作成する
func createTestSecondaryIndex(t *testing.T, bp *buffer.BufferPool, skCount int, unique bool) (*SecondaryIndex, error) {
	t.Helper()
	return CreateSecondaryIndex(bp, page.FileId(0), skCount, unique)
}

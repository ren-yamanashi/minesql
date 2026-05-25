package btree

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewTree(t *testing.T) {
	t.Run("既存の B+Tree を開ける", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		created, _ := CreateTree(bp, page.FileId(0))

		// WHEN
		bt := NewTree(bp, created.MetaPageId())

		// THEN
		assert.Equal(t, created.MetaPageId(), bt.MetaPageId())
	})

	t.Run("NewTree で開いた B+Tree のメタデータを読み取れる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		created, _ := CreateTree(bp, page.FileId(0))

		// WHEN
		bt := NewTree(bp, created.MetaPageId())
		count, err := bt.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestCreateTree(t *testing.T) {
	t.Run("B+Tree を作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)

		// WHEN
		bt, err := CreateTree(bp, page.FileId(0))

		// THEN
		assert.NoError(t, err)
		assert.False(t, bt.MetaPageId().IsInvalid())
	})

	t.Run("作成後のリーフページ数は 1 になる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)

		// WHEN
		bt, err := CreateTree(bp, page.FileId(0))
		assert.NoError(t, err)
		count, err := bt.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})

	t.Run("作成後の高さは 1 になる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)

		// WHEN
		bt, err := CreateTree(bp, page.FileId(0))
		assert.NoError(t, err)
		height, err := bt.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

func TestLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))

		// WHEN
		count, err := bt.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestHeight(t *testing.T) {
	t.Run("B+Tree の高さを取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))

		// WHEN
		height, err := bt.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// setupBtreeTestBufferPool はテスト用のバッファプールを作成する
func setupBtreeTestBufferPool(t *testing.T) *buffer.Pool {
	t.Helper()
	bp := newTestBufferPool(page.Size * 10)
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(0, path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(0, hf)
	return bp
}

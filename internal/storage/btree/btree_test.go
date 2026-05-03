package btree

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreateBtree(t *testing.T) {
	t.Run("B+Tree を作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)

		// WHEN
		bt, err := CreateBtree(bp, page.FileId(0))

		// THEN
		assert.NoError(t, err)
		assert.False(t, bt.metaPageId.IsInvalid())
	})

	t.Run("作成後のリーフページ数は 1 になる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)

		// WHEN
		bt, err := CreateBtree(bp, page.FileId(0))
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
		bt, err := CreateBtree(bp, page.FileId(0))
		assert.NoError(t, err)
		height, err := bt.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

func TestNewBtree(t *testing.T) {
	t.Run("既存の B+Tree を開ける", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		created, _ := CreateBtree(bp, page.FileId(0))

		// WHEN
		bt := NewBtree(bp, created.metaPageId)

		// THEN
		assert.Equal(t, created.metaPageId, bt.metaPageId)
	})

	t.Run("NewBtree で開いた B+Tree のメタデータを読み取れる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		created, _ := CreateBtree(bp, page.FileId(0))

		// WHEN
		bt := NewBtree(bp, created.metaPageId)
		count, err := bt.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateBtree(bp, page.FileId(0))

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
		bt, _ := CreateBtree(bp, page.FileId(0))

		// WHEN
		height, err := bt.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// setupBtreeTestBufferPool はテスト用のバッファプールを作成する
func setupBtreeTestBufferPool(t *testing.T) *buffer.BufferPool {
	t.Helper()
	bp := buffer.NewBufferPool(page.PageSize * 10)
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(0, path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(0, hf)
	return bp
}

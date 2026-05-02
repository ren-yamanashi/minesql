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
		metaPageId, _ := bp.AllocatePageId(0)

		// WHEN
		bt, err := CreateBtree(bp, metaPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, metaPageId, bt.MetaPageId)
	})

	t.Run("作成後のリーフページ数は 1 になる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)

		// WHEN
		bt, err := CreateBtree(bp, metaPageId)
		assert.NoError(t, err)
		count, err := bt.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})

	t.Run("作成後の高さは 1 になる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)

		// WHEN
		bt, err := CreateBtree(bp, metaPageId)
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
		metaPageId, _ := bp.AllocatePageId(0)
		CreateBtree(bp, metaPageId)

		// WHEN
		bt := NewBtree(bp, metaPageId)

		// THEN
		assert.Equal(t, metaPageId, bt.MetaPageId)
	})

	t.Run("NewBtree で開いた B+Tree のメタデータを読み取れる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		metaPageId, _ := bp.AllocatePageId(0)
		CreateBtree(bp, metaPageId)

		// WHEN
		bt := NewBtree(bp, metaPageId)
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
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)

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
		metaPageId, _ := bp.AllocatePageId(0)
		bt, _ := CreateBtree(bp, metaPageId)

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

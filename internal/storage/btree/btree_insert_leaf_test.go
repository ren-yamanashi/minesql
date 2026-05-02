package btree

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestInsertLeaf(t *testing.T) {
	t.Run("リーフノードにレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		pageId, pg := setupTestLeafPage(t, bp)
		record := node.NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA})

		// WHEN
		overflowKey, newPageId, err := bt.insertLeaf(pageId, pg, record)

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, overflowKey)
		assert.True(t, newPageId.IsInvalid())
	})

	t.Run("複数のレコードをソート順に挿入できる", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		pageId, pg := setupTestLeafPage(t, bp)

		// WHEN
		bt.insertLeaf(pageId, pg, node.NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))
		bt.insertLeaf(pageId, pg, node.NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// THEN
		leafNode := node.NewLeafNode(pg)
		assert.Equal(t, 2, leafNode.NumRecords())
		assert.Equal(t, []byte{0x10}, leafNode.Record(0).Key())
		assert.Equal(t, []byte{0x20}, leafNode.Record(1).Key())
	})

	t.Run("重複キーの場合は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		pageId, pg := setupTestLeafPage(t, bp)
		bt.insertLeaf(pageId, pg, node.NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		_, _, err := bt.insertLeaf(pageId, pg, node.NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xBB}))

		// THEN
		assert.ErrorIs(t, err, ErrDuplicateKey)
	})

	t.Run("リーフノードが満杯の場合は分割される", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		pageId, pg := setupTestLeafPage(t, bp)
		nonKey := make([]byte, 1500)
		for i := range 2 {
			key := []byte{byte(i + 1)}
			bt.insertLeaf(pageId, pg, node.NewRecord([]byte{0x01}, key, nonKey))
		}

		// WHEN
		overflowKey, newPageId, err := bt.insertLeaf(pageId, pg, node.NewRecord([]byte{0x01}, []byte{0xFF}, nonKey))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, overflowKey)
		assert.False(t, newPageId.IsInvalid())
	})
}

// setupBtreeBufferPool はテスト用のバッファプールを作成する
func setupBtreeBufferPool(t *testing.T) *buffer.BufferPool {
	t.Helper()
	bp := buffer.NewBufferPool(page.PageSize * 20)
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(0, path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(0, hf)
	return bp
}

// setupBtreeForTest はテスト用の Btree とバッファプールを返す
func setupBtreeForTest(t *testing.T) (*Btree, *buffer.BufferPool) {
	t.Helper()
	bp := setupBtreeBufferPool(t)
	metaPageId, err := bp.AllocatePageId(0)
	assert.NoError(t, err)
	bt, err := CreateBtree(bp, metaPageId)
	assert.NoError(t, err)
	return bt, bp
}

// setupTestLeafPage はテスト用のリーフページを作成し、PageId と Page を返す
func setupTestLeafPage(t *testing.T, bp *buffer.BufferPool) (page.PageId, *page.Page) {
	t.Helper()
	pageId, err := bp.AllocatePageId(0)
	assert.NoError(t, err)
	_, err = bp.AddPage(pageId)
	assert.NoError(t, err)
	pg, err := bp.GetWritePage(pageId)
	assert.NoError(t, err)
	ln := node.NewLeafNode(pg)
	ln.Initialize()
	return pageId, pg
}

package btree

import (
	"path/filepath"
	"testing"

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
		record := NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA})

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
		_, _, _ = bt.insertLeaf(pageId, pg, NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))
		_, _, _ = bt.insertLeaf(pageId, pg, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// THEN
		leafNode := newLeafNode(pg)
		assert.Equal(t, 2, leafNode.numRecords())
		assert.Equal(t, []byte{0x10}, leafNode.record(0).Key())
		assert.Equal(t, []byte{0x20}, leafNode.record(1).Key())
	})

	t.Run("重複キーの場合は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		bt, bp := setupBtreeForTest(t)
		pageId, pg := setupTestLeafPage(t, bp)
		_, _, _ = bt.insertLeaf(pageId, pg, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))

		// WHEN
		_, _, err := bt.insertLeaf(pageId, pg, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xBB}))

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
			_, _, _ = bt.insertLeaf(pageId, pg, NewRecord([]byte{0x01}, key, nonKey))
		}

		// WHEN
		overflowKey, newPageId, err := bt.insertLeaf(pageId, pg, NewRecord([]byte{0x01}, []byte{0xFF}, nonKey))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, overflowKey)
		assert.False(t, newPageId.IsInvalid())
	})
}

// setupBtreeBufferPool はテスト用のバッファプールを作成する
func setupBtreeBufferPool(t *testing.T) *buffer.Pool {
	t.Helper()
	bp := buffer.NewPool(page.Size * 20)
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(0, path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(0, hf)
	return bp
}

// setupBtreeForTest はテスト用の Tree とバッファプールを返す
func setupBtreeForTest(t *testing.T) (*Tree, *buffer.Pool) {
	t.Helper()
	bp := setupBtreeBufferPool(t)
	bt, err := CreateTree(bp, page.FileId(0))
	assert.NoError(t, err)
	return bt, bp
}

// setupTestLeafPage はテスト用のリーフページを作成し、PageId と Page を返す
func setupTestLeafPage(t *testing.T, bp *buffer.Pool) (page.Id, *page.Page) {
	t.Helper()
	pageId, err := bp.AllocatePageId(0)
	assert.NoError(t, err)
	_, err = bp.AddPage(pageId)
	assert.NoError(t, err)
	bufPage, err := bp.PageForWrite(pageId)
	assert.NoError(t, err)
	ln := newLeafNode(bufPage.Page)
	ln.initialize()
	return pageId, bufPage.Page
}

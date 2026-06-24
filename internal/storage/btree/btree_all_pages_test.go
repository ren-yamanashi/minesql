package btree

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestAllPageIds(t *testing.T) {
	t.Run("空の B+Tree (高さ 1) ではメタページとルートリーフを返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewWriteMtr(bt.bufferPool, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		defer mtr.UnpinAll()

		// WHEN
		ids, err := bt.AllPageIds(mtr)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(ids))
		assert.Equal(t, bt.MetaPageId(), ids[0])
		assert.False(t, ids[1].IsInvalid())
	})

	t.Run("高さ 2 の B+Tree ではメタページ・ルートブランチ・全リーフを返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))
		height, _ := bt.Height(mtr)
		assert.Equal(t, uint64(2), height)
		leafCount, _ := bt.LeafPageCount(mtr)

		// WHEN
		ids, err := bt.AllPageIds(mtr)

		// THEN
		assert.NoError(t, err)
		// メタページ 1 + ルートブランチ 1 + リーフ数
		expectedCount := 2 + int(leafCount)
		assert.Equal(t, expectedCount, len(ids))
		assert.Equal(t, bt.MetaPageId(), ids[0])
		for _, id := range ids {
			assert.False(t, id.IsInvalid())
		}
		assertUniquePageIds(t, ids)
	})

	t.Run("複数階層の B+Tree でも全ページを返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		for i := range 30 {
			_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{byte(i)}, nonKey))
		}
		height, _ := bt.Height(mtr)
		assert.GreaterOrEqual(t, height, uint64(2))

		// WHEN
		ids, err := bt.AllPageIds(mtr)

		// THEN
		assert.NoError(t, err)
		leafCount, _ := bt.LeafPageCount(mtr)
		// 全ページ数 >= メタ + ルート + リーフ数
		assert.GreaterOrEqual(t, len(ids), 2+int(leafCount))
		assert.Equal(t, bt.MetaPageId(), ids[0])
		for _, id := range ids {
			assert.False(t, id.IsInvalid())
		}
		assertUniquePageIds(t, ids)
	})

	t.Run("高さ 3 以上の B+Tree でも内部ノード階層を全て返す", func(t *testing.T) {
		// GIVEN
		bp := newLargeBtreeTestBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		nonKey := make([]byte, 1500)
		key := make([]byte, 200)
		for i := range 50 {
			key[0] = byte(i)
			mtr := buffer.NewMtr(bt.bufferPool)
			_ = bt.Insert(mtr, NewRecord([]byte{}, key, nonKey))
			mtr.UnpinAll()
		}
		writeMtr := buffer.NewWriteMtr(bt.bufferPool, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		defer writeMtr.UnpinAll()
		height, _ := bt.Height(writeMtr)
		assert.GreaterOrEqual(t, height, uint64(3))
		leafCount, _ := bt.LeafPageCount(writeMtr)

		// WHEN
		ids, err := bt.AllPageIds(writeMtr)

		// THEN
		assert.NoError(t, err)
		// メタ + 内部ノード階層 (>=2 つ) + リーフ
		assert.Greater(t, len(ids), 2+int(leafCount))
		assert.Equal(t, bt.MetaPageId(), ids[0])
		assertUniquePageIds(t, ids)
	})

	t.Run("セカンダリインデックス相当の B+Tree でも全ページ列挙できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB, 0xBB}))

		// WHEN
		ids, err := bt.AllPageIds(mtr)

		// THEN
		assert.NoError(t, err)
		// 高さ 1 → メタ + ルートリーフのみ
		assert.Equal(t, 2, len(ids))
		assertUniquePageIds(t, ids)
	})

	t.Run("メタページが BFS 順で先頭、リーフが末尾になる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		nonKey := make([]byte, 1500)
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x01}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x02}, nonKey))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x03}, nonKey))

		// WHEN
		ids, err := bt.AllPageIds(mtr)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, bt.MetaPageId(), ids[0])
		leafIds, leafErr := bt.leafPageIds(mtr)
		assert.NoError(t, leafErr)
		tail := ids[len(ids)-len(leafIds):]
		assertSamePageIdSet(t, leafIds, tail)
	})
}

func newLargeBtreeTestBufferPool(t *testing.T) *buffer.Pool {
	t.Helper()
	bp := newTestBufferPool(t, page.Size*200)
	path := filepath.Join(t.TempDir(), "test_large.db")
	hf, err := file.NewHeapFile(0, path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(0, hf)
	return bp
}

func assertUniquePageIds(t *testing.T, ids []page.Id) {
	t.Helper()
	seen := make(map[page.Id]struct{}, len(ids))
	for _, id := range ids {
		_, dup := seen[id]
		assert.False(t, dup, "duplicate page id: %v", id)
		seen[id] = struct{}{}
	}
}

func assertSamePageIdSet(t *testing.T, expected, actual []page.Id) {
	t.Helper()
	expectedSet := make(map[page.Id]struct{}, len(expected))
	for _, id := range expected {
		expectedSet[id] = struct{}{}
	}
	actualSet := make(map[page.Id]struct{}, len(actual))
	for _, id := range actual {
		actualSet[id] = struct{}{}
	}
	assert.Equal(t, expectedSet, actualSet)
}

package btree

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIteratorGet(t *testing.T) {
	t.Run("現在のスロットのレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tree, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		})
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		bufPage, _ := tree.bufferPool.Page(pageId)
		iter := NewIterator(tree, mtr, bufPage, 0, SearchModeStart{})

		// WHEN
		record, ok, err := iter.Get()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x10}, record.Key())
	})

	t.Run("スロット番号がレコード数以上の場合は false を返す", func(t *testing.T) {
		// GIVEN
		tree, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		})
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		bufPage, _ := tree.bufferPool.Page(pageId)
		iter := NewIterator(tree, mtr, bufPage, 1, SearchModeStart{})

		// WHEN
		_, ok, err := iter.Get()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestIteratorNext(t *testing.T) {
	t.Run("レコードを取得して次に進む", func(t *testing.T) {
		// GIVEN
		tree, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
			ln.insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))
		})
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		bufPage, _ := tree.bufferPool.Page(pageId)
		iter := NewIterator(tree, mtr, bufPage, 0, SearchModeStart{})

		// WHEN
		record1, ok1, err1 := iter.Next()
		record2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, []byte{0x10}, record1.Key())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, []byte{0x20}, record2.Key())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

}

func TestIteratorAdvance(t *testing.T) {
	t.Run("同一ページ内の次のスロットに進む", func(t *testing.T) {
		// GIVEN
		tree, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
			ln.insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))
		})
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		bufPage, _ := tree.bufferPool.Page(pageId)
		iter := NewIterator(tree, mtr, bufPage, 0, SearchModeStart{})

		// WHEN
		err := iter.Advance()

		// THEN
		assert.NoError(t, err)
		record, ok, _ := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, []byte{0x20}, record.Key())
	})

	t.Run("現在のページを読み終えたら次のページに遷移する", func(t *testing.T) {
		// GIVEN
		bp := newTestBufferPool(t, page.Size*10)
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := file.NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { _ = hf.Close() })
		bp.RegisterHeapFile(0, hf)

		tree, err := createTreeForTest(t, bp, 0)
		assert.NoError(t, err)

		allocMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		firstId, err := fsp.AllocatePage(allocMtr, page.FileId(0))
		assert.NoError(t, err)
		secondId, err := fsp.AllocatePage(allocMtr, page.FileId(0))
		assert.NoError(t, err)
		assert.NoError(t, allocMtr.Commit())

		firstPage, err := bp.AddPage(firstId)
		assert.NoError(t, err)
		firstLeaf := newLeafNode(firstPage)
		firstLeaf.initialize()
		firstLeaf.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		firstLeaf.setNextPageId(secondId)

		secondPage, err := bp.AddPage(secondId)
		assert.NoError(t, err)
		secondLeaf := newLeafNode(secondPage)
		secondLeaf.initialize()
		secondLeaf.insert(0, NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))

		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		bufPage, err := mtr.PageForRead(firstId)
		assert.NoError(t, err)
		iter := NewIterator(tree, mtr, bufPage, 0, SearchModeStart{})

		// WHEN
		err = iter.Advance()

		// THEN
		assert.NoError(t, err)
		record, ok, _ := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, []byte{0x20}, record.Key())
		assert.Equal(t, secondId, iter.bufferPage.PageId())
	})
}

// setupIteratorTestPage はテスト用のバッファプールとリーフページを作成する
func TestIteratorTracksLastKeyAndModifyCount(t *testing.T) {
	t.Run("Get 呼び出し後に lastKey と modifyCountSnap が更新される", func(t *testing.T) {
		// GIVEN
		tree, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		})
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		bufPage, _ := tree.bufferPool.Page(pageId)
		iter := NewIterator(tree, mtr, bufPage, 0, SearchModeStart{})
		assert.Nil(t, iter.lastKey)

		// WHEN
		_, ok, err := iter.Get()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x10}, iter.lastKey)
		assert.Equal(t, bufPage.ModifyCount(), iter.modifyCountSnap)
	})
}

func TestIteratorRefetchByKey(t *testing.T) {
	t.Run("lastKey が残っているとき次のスロットに進む", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))
		iter, _ := bt.Search(mtr, SearchModeStart{})
		_, _, _ = iter.Get() // lastKey = 0x10

		// WHEN
		err := iter.refetchByKey([]byte{0x10})

		// THEN
		assert.NoError(t, err)
		record, ok, err := iter.Get()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x20}, record.Key())
	})

	t.Run("modifyCount 変化後の Advance は二重インクリメントしない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x30}, []byte{0xCC}))
		iter, _ := bt.Search(mtr, SearchModeStart{})
		_, _, _ = iter.Get()
		_ = bt.Update(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xFF}))

		// WHEN
		err := iter.Advance()

		// THEN
		assert.NoError(t, err)
		record, ok, err := iter.Get()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x20}, record.Key())
	})

	t.Run("lastKey が削除されているとき同じ slotNum をそのまま使う", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))
		iter, _ := bt.Search(mtr, SearchModeStart{})
		_, _, _ = iter.Get()
		// 0x10 を削除して lastKey 不在の状態を作る
		_ = bt.Delete(mtr, []byte{0x10})

		// WHEN
		err := iter.refetchByKey([]byte{0x10})

		// THEN
		assert.NoError(t, err)
		record, ok, err := iter.Get()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x20}, record.Key())
	})
}

func TestIteratorRefetchBySearchMode(t *testing.T) {
	t.Run("SearchModeKey で取得した Iterator は初回 Get 前にリーフに挿入が入ると SearchMode で再降下して正しいキーを返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA}))
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		iter, err := bt.Search(mtr, SearchModeKey{Key: []byte{0x20}})
		assert.NoError(t, err)

		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x15}, []byte{0xCC}))

		// WHEN
		record, ok, err := iter.Get()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x20}, record.Key())
	})

	t.Run("SearchModeStart で取得した Iterator は初回 Get 前にリーフに挿入が入ると SearchMode で再降下して新しい先頭キーを返す", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB}))

		iter, err := bt.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		snapBefore := iter.modifyCountSnap

		_ = bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xCC}))

		// WHEN
		record, ok, err := iter.Get()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x10}, record.Key())
		assert.NotEqual(t, snapBefore, iter.modifyCountSnap)
	})
}

func setupIteratorTestPage(t *testing.T, setup func(ln *leafNode)) (*Tree, page.Id) {
	t.Helper()

	bp := newTestBufferPool(t, page.Size*10)
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(0, hf)

	// refetch 経路に必要な Tree (テストでは modifyCount を進めないため Search は呼ばれない)
	tree, err := createTreeForTest(t, bp, 0)
	assert.NoError(t, err)

	allocMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
	pageId, err := fsp.AllocatePage(allocMtr, page.FileId(0))
	assert.NoError(t, err)
	assert.NoError(t, allocMtr.Commit())

	bufPage, err := bp.AddPage(pageId)
	assert.NoError(t, err)

	ln := newLeafNode(bufPage)
	ln.initialize()
	setup(ln)

	return tree, pageId
}

func TestIteratorMtr(t *testing.T) {
	t.Run("Mtr getter は NewIterator に渡した mtr を返す", func(t *testing.T) {
		// GIVEN
		tree, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		})
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		bufPage, _ := tree.bufferPool.Page(pageId)

		// WHEN
		iter := NewIterator(tree, mtr, bufPage, 0, SearchModeStart{})

		// THEN
		assert.Same(t, mtr, iter.Mtr())
	})
}

func TestIteratorBufferPageId(t *testing.T) {
	t.Run("BufferPageId getter は現在参照しているリーフページの ID を返す", func(t *testing.T) {
		// GIVEN
		tree, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		})
		mtr := buffer.NewMtr(tree.bufferPool)
		defer mtr.UnpinAll()
		bufPage, _ := tree.bufferPool.Page(pageId)

		// WHEN
		iter := NewIterator(tree, mtr, bufPage, 0, SearchModeStart{})

		// THEN
		assert.Equal(t, pageId, iter.BufferPageId())
	})
}

func TestIteratorConcurrentMerge(t *testing.T) {
	t.Run("隣接 leaf が merge で解放されても Advance で全レコードを読み切れる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		nonKey := make([]byte, 1500)
		keys := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
		for _, k := range keys {
			require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{k}, nonKey)))
		}
		require.NoError(t, insertMtr.Commit())

		iterMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		iter, err := bt.Search(iterMtr, SearchModeStart{})
		require.NoError(t, err)

		first, ok, err := iter.Next()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, []byte{0x01}, first.Key())

		// WHEN
		require.NoError(t, bt.Delete(iterMtr, []byte{0x05}))

		// THEN
		var readKeys []byte
		for {
			r, ok, err := iter.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			readKeys = append(readKeys, r.Key()[0])
		}
		assert.Equal(t, []byte{0x02, 0x03, 0x04}, readKeys)
		require.NoError(t, iterMtr.Commit())
	})

	t.Run("merge で解放されたページが再割り当てされ中身が変わっても refetch が正しく再位置付けする", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		nonKey := make([]byte, 1500)
		keys := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
		for _, k := range keys {
			require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{k}, nonKey)))
		}
		require.NoError(t, insertMtr.Commit())

		iterMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		iter, err := bt.Search(iterMtr, SearchModeStart{})
		require.NoError(t, err)

		first, ok, err := iter.Next()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, []byte{0x01}, first.Key())

		// WHEN
		require.NoError(t, bt.Delete(iterMtr, []byte{0x05}))
		require.NoError(t, bt.Insert(iterMtr, NewRecord([]byte{}, []byte{0x99}, nonKey)))

		// THEN
		var readKeys []byte
		for {
			r, ok, err := iter.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			readKeys = append(readKeys, r.Key()[0])
		}
		assert.Equal(t, []byte{0x02, 0x03, 0x04, 0x99}, readKeys)
		require.NoError(t, iterMtr.Commit())
	})

	t.Run("走査中の leaf 自体が merge で survivor に吸収されても refetch が survivor 上のレコードへ再位置付けする", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		nonKey := make([]byte, 1500)
		keys := []byte{0x01, 0x02, 0x03}
		for _, k := range keys {
			require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{k}, nonKey)))
		}
		require.NoError(t, insertMtr.Commit())

		iterMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		iter, err := bt.Search(iterMtr, SearchModeKey{Key: []byte{0x02}})
		require.NoError(t, err)

		second, ok, err := iter.Next()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, []byte{0x02}, second.Key())

		iterLeafPageIdBefore := iter.BufferPageId()

		// WHEN
		require.NoError(t, bt.Delete(iterMtr, []byte{0x03}))

		isFree, err := fsp.IsPageFree(iterMtr, iterLeafPageIdBefore)
		require.NoError(t, err)
		require.True(t, isFree)

		// THEN
		var readKeys []byte
		for {
			r, ok, err := iter.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			readKeys = append(readKeys, r.Key()[0])
		}
		assert.Empty(t, readKeys)
		assert.NotEqual(t, iterLeafPageIdBefore, iter.BufferPageId())
		require.NoError(t, iterMtr.Commit())
	})
}

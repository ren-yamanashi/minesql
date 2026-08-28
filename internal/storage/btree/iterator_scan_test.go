package btree

import (
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanIteratorNext(t *testing.T) {
	t.Run("全件走査で挿入順どおりの結果が得られる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		keys := []byte{0x10, 0x20, 0x30, 0x40, 0x50}
		for _, k := range keys {
			require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{k}, []byte{k})))
		}
		require.NoError(t, insertMtr.Commit())

		// WHEN
		iter, err := bt.OpenScan(SearchModeStart{})
		require.NoError(t, err)

		var got []byte
		for {
			rec, mtr, ok, err := iter.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			got = append(got, rec.Key()[0])
			mtr.UnpinAll()
		}

		// THEN
		assert.Equal(t, keys, got)
	})

	t.Run("走査の合間に他 mtr がページ分割してもカウンタ照合 + lastKey 再降下で継続できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		nonKey := make([]byte, 1500)
		initialKeys := []byte{0x10, 0x20, 0x30}
		for _, k := range initialKeys {
			require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{k}, nonKey)))
		}
		require.NoError(t, insertMtr.Commit())

		iter, err := bt.OpenScan(SearchModeStart{})
		require.NoError(t, err)

		// WHEN
		rec1, mtr1, ok, err := iter.Next()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, []byte{0x10}, rec1.Key())
		mtr1.UnpinAll()

		// 走査の合間 (呼び出し側 mtr close 済) にページ分割を起こす
		splitMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		for _, k := range []byte{0x40, 0x50, 0x60, 0x70, 0x80} {
			require.NoError(t, bt.Insert(splitMtr, NewRecord([]byte{}, []byte{k}, nonKey)))
		}
		require.NoError(t, splitMtr.Commit())

		var rest []byte
		for {
			rec, mtr, ok, err := iter.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			rest = append(rest, rec.Key()[0])
			mtr.UnpinAll()
		}

		// THEN
		assert.Equal(t, []byte{0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80}, rest)
	})

	t.Run("走査の合間に lastKey が削除されても位置回復して継続できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		keys := []byte{0x10, 0x20, 0x30, 0x40}
		for _, k := range keys {
			require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{k}, []byte{k})))
		}
		require.NoError(t, insertMtr.Commit())

		iter, err := bt.OpenScan(SearchModeStart{})
		require.NoError(t, err)

		// WHEN
		rec1, mtr1, ok, err := iter.Next()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, []byte{0x10}, rec1.Key())
		mtr1.UnpinAll()

		// lastKey = 0x10 を他 mtr で削除
		delMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		require.NoError(t, bt.Delete(delMtr, []byte{0x10}))
		require.NoError(t, delMtr.Commit())

		var rest []byte
		for {
			rec, mtr, ok, err := iter.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			rest = append(rest, rec.Key()[0])
			mtr.UnpinAll()
		}

		// THEN
		assert.Equal(t, []byte{0x20, 0x30, 0x40}, rest)
	})

	t.Run("複数ページに跨る走査でも全件を昇順で取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		nonKey := make([]byte, 1500)
		keys := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		for _, k := range keys {
			require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{k}, nonKey)))
		}
		require.NoError(t, insertMtr.Commit())

		// WHEN
		iter, err := bt.OpenScan(SearchModeStart{})
		require.NoError(t, err)

		var got []byte
		for {
			rec, mtr, ok, err := iter.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			got = append(got, rec.Key()[0])
			mtr.UnpinAll()
		}

		// THEN
		assert.Equal(t, keys, got)
	})

	t.Run("終端到達で done が立ち mtr は nil で返る", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA})))
		require.NoError(t, insertMtr.Commit())

		iter, err := bt.OpenScan(SearchModeStart{})
		require.NoError(t, err)

		// WHEN
		_, mtr1, ok, err := iter.Next()
		require.NoError(t, err)
		require.True(t, ok)
		mtr1.UnpinAll()

		_, mtr2, ok, err := iter.Next()

		// THEN
		require.NoError(t, err)
		require.False(t, ok)
		assert.Nil(t, mtr2)
		assert.True(t, iter.done)
	})

	t.Run("Next が返った後 (呼び出し側 mtr close 後) は別 mtr が同ページに X ラッチを取れる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA})))
		require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB})))
		require.NoError(t, insertMtr.Commit())

		iter, err := bt.OpenScan(SearchModeStart{})
		require.NoError(t, err)
		leafPageId := iter.pinnedPageId

		// WHEN
		_, mtr, ok, err := iter.Next()
		require.NoError(t, err)
		require.True(t, ok)
		mtr.UnpinAll()

		// THEN: mtr close 後は同一リーフの X ラッチを別 mtr が即時取得できる
		writer := buffer.NewMtr(bp)
		acquired := make(chan struct{})
		go func() {
			_, _ = writer.PageForWrite(leafPageId)
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatal("走査中の mtr 解放後もリーフの S ラッチが残っている (走査側が保持しっぱなし)")
		}
		writer.UnpinAll()
		iter.Close()
	})

	t.Run("ページ跨ぎ直後に次リーフへ変更が入っても残レコードを全件読める", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		nonKey := make([]byte, 1500)
		// 3 リーフ以上に分割される件数を確保する
		initialKeys := []byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90}
		for _, k := range initialKeys {
			require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{k}, nonKey)))
		}
		require.NoError(t, insertMtr.Commit())

		iter, err := bt.OpenScan(SearchModeStart{})
		require.NoError(t, err)

		var got []byte
		injected := false
		var deletedKey byte
		for {
			prevPageId := iter.pinnedPageId
			rec, mtr, ok, err := iter.Next()
			require.NoError(t, err)
			if !ok {
				break
			}
			got = append(got, rec.Key()[0])
			mtr.UnpinAll()

			// WHEN: 跨ぎ直後 (= 新リーフへ位置付けた直後、lastKey = 旧リーフ末尾) に、新 pinned リーフ内の未走査レコードを削除して modifyCount を進める
			if !injected && iter.pinnedPageId != prevPageId {
				pickMtr := buffer.NewMtr(bp)
				pinPage, perr := pickMtr.PageForRead(iter.pinnedPageId)
				require.NoError(t, perr)
				leaf := newLeafNode(pinPage)
				require.Greater(t, leaf.numRecords(), iter.slotNum+1, "テスト前提: 削除余地がある新リーフ")
				deletedKey = leaf.record(iter.slotNum + 1).Key()[0]
				pickMtr.UnpinAll()

				delMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
				require.NoError(t, bt.Delete(delMtr, []byte{deletedKey}))
				require.NoError(t, delMtr.Commit())
				injected = true
			}
		}

		// THEN: 挿入前と同じキー集合 (削除分を除く) を昇順で読めていること + 走査終了後リーフのラッチが残っていないこと
		require.True(t, injected, "テスト前提: 走査中にリーフ跨ぎが発生していること")
		expected := make([]byte, 0, len(initialKeys)-1)
		for _, k := range initialKeys {
			if k == deletedKey {
				continue
			}
			expected = append(expected, k)
		}
		assert.Equal(t, expected, got)
		assert.True(t, iter.done)

		writer := buffer.NewMtr(bp)
		acquired := make(chan struct{})
		go func() {
			_, _ = writer.PageForWrite(iter.pinnedPageId)
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatal("走査終了後もリーフのラッチが残っている")
		}
		writer.UnpinAll()
	})
}

func TestScanIteratorClose(t *testing.T) {
	t.Run("走査途中で Close を呼ぶと done が立ち再 Close は無害", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		bt, _ := createTreeForTest(t, bp, page.FileId(0))
		insertMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		for _, k := range []byte{0x10, 0x20, 0x30} {
			require.NoError(t, bt.Insert(insertMtr, NewRecord([]byte{}, []byte{k}, []byte{k})))
		}
		require.NoError(t, insertMtr.Commit())

		iter, err := bt.OpenScan(SearchModeStart{})
		require.NoError(t, err)
		assert.False(t, iter.done)

		// WHEN
		iter.Close()

		// THEN
		assert.True(t, iter.done)

		// 再 Close は無害
		iter.Close()
		assert.True(t, iter.done)
	})
}

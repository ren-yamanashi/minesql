package fsp

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestXdesEntryOffset(t *testing.T) {
	t.Run("エントリインデックスからボディ相対オフセットを算出する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry0 := writableEntry(t, mtr, 0)
		entry2 := writableEntry(t, mtr, 2)

		// WHEN
		off0 := entry0.offset()
		off2 := entry2.offset()

		// THEN
		assert.Equal(t, 108, off0)
		assert.Equal(t, 108+2*56, off2)
		commitMtr(t, mtr)
	})
}

func TestXdesEntrySegmentId(t *testing.T) {
	t.Run("setSegmentId で書き込んだ id を読み取れる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.setSegmentId(42)

		// WHEN
		got := entry.segmentId()

		// THEN
		assert.Equal(t, uint64(42), got)
		commitMtr(t, mtr)
	})

	t.Run("初期化直後は 0 を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// WHEN
		got := entry.segmentId()

		// THEN
		assert.Equal(t, uint64(0), got)
		commitMtr(t, mtr)
	})
}

func TestXdesEntrySetSegmentId(t *testing.T) {
	t.Run("書き込んだ id が segmentId で復元できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)

		// WHEN
		entry.setSegmentId(99)

		// THEN
		assert.Equal(t, uint64(99), entry.segmentId())
		commitMtr(t, mtr)
	})
}

func TestXdesEntryIsLeasable(t *testing.T) {
	t.Run("記述子ページを先頭に含み先頭 1 ページのみ使用中の extent は true を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		entry.setPageFree(0, false)

		// WHEN
		got := entry.isLeasable()

		// THEN
		assert.True(t, got)
		commitMtr(t, mtr)
	})

	t.Run("記述子ページを先頭に含まない extent は false を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 1)
		entry.initialize()
		entry.setPageFree(0, false)

		// WHEN
		got := entry.isLeasable()

		// THEN
		assert.False(t, got)
		commitMtr(t, mtr)
	})

	t.Run("先頭ページが free の場合は false を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// WHEN
		got := entry.isLeasable()

		// THEN
		assert.False(t, got)
		commitMtr(t, mtr)
	})

	t.Run("予約分以外に使用中ページがある場合は false を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		entry.setPageFree(0, false)
		entry.setPageFree(5, false)

		// WHEN
		got := entry.isLeasable()

		// THEN
		assert.False(t, got)
		commitMtr(t, mtr)
	})
}

func TestXdesEntryState(t *testing.T) {
	t.Run("初期化前は NOT_INITED を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)

		// WHEN
		got := entry.state()

		// THEN
		assert.Equal(t, stateNotInited, got)
		commitMtr(t, mtr)
	})

	t.Run("setState で書き込んだ状態を読み取れる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		entry.setState(stateFreeFrag)

		// WHEN
		got := entry.state()

		// THEN
		assert.Equal(t, stateFreeFrag, got)
		commitMtr(t, mtr)
	})
}

func TestXdesEntrySetState(t *testing.T) {
	t.Run("許容遷移で状態を設定できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// WHEN
		entry.setState(stateFreeFrag)
		entry.setState(stateFullFrag)
		entry.setState(stateFreeFrag)
		entry.setState(stateFree)

		// THEN
		assert.Equal(t, stateFree, entry.state())
		commitMtr(t, mtr)
	})

	t.Run("許容されない遷移は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// THEN
		assert.Panics(t, func() { entry.setState(stateFullFrag) })
		commitMtr(t, mtr)
	})
}

func TestXdesEntryIsPageFree(t *testing.T) {
	t.Run("初期化直後は全ページが free と判定される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// THEN
		assert.True(t, entry.isPageFree(0))
		assert.True(t, entry.isPageFree(extentPageCount-1))
		commitMtr(t, mtr)
	})

	t.Run("setPageFree で used にした位置は free と判定されない", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		entry.setPageFree(0, false)
		entry.setPageFree(extentPageCount-1, false)

		// THEN
		assert.False(t, entry.isPageFree(0))
		assert.False(t, entry.isPageFree(extentPageCount-1))
		assert.True(t, entry.isPageFree(1))
		assert.True(t, entry.isPageFree(extentPageCount-2))
		commitMtr(t, mtr)
	})

	t.Run("範囲外の位置は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// THEN
		assert.Panics(t, func() { entry.isPageFree(-1) })
		assert.Panics(t, func() { entry.isPageFree(extentPageCount) })
		commitMtr(t, mtr)
	})
}

func TestXdesEntrySetPageFree(t *testing.T) {
	t.Run("境界位置 0 と 255 の free bit を書き換えられる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// WHEN
		entry.setPageFree(0, false)
		entry.setPageFree(extentPageCount-1, false)

		// THEN
		assert.False(t, entry.isPageFree(0))
		assert.False(t, entry.isPageFree(extentPageCount-1))
		commitMtr(t, mtr)
	})

	t.Run("used に設定した位置を free に戻せる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		entry.setPageFree(0, false)

		// WHEN
		entry.setPageFree(0, true)

		// THEN
		assert.True(t, entry.isPageFree(0))
		commitMtr(t, mtr)
	})

	t.Run("範囲外の位置は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// THEN
		assert.Panics(t, func() { entry.setPageFree(extentPageCount, true) })
		commitMtr(t, mtr)
	})
}

func TestXdesEntryIsAllFree(t *testing.T) {
	t.Run("初期化直後は全ページ free で true", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// THEN
		assert.True(t, entry.isAllFree())
		commitMtr(t, mtr)
	})

	t.Run("used が混在すると false", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		entry.setPageFree(0, false)

		// THEN
		assert.False(t, entry.isAllFree())
		commitMtr(t, mtr)
	})

	t.Run("全ページ used で false", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		for pos := range extentPageCount {
			entry.setPageFree(pos, false)
		}

		// THEN
		assert.False(t, entry.isAllFree())
		commitMtr(t, mtr)
	})
}

func TestXdesEntryIsAllUsed(t *testing.T) {
	t.Run("初期化直後は全ページ free で false", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// THEN
		assert.False(t, entry.isAllUsed())
		commitMtr(t, mtr)
	})

	t.Run("used が混在すると false", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		entry.setPageFree(0, false)

		// THEN
		assert.False(t, entry.isAllUsed())
		commitMtr(t, mtr)
	})

	t.Run("全ページ used で true", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		for pos := range extentPageCount {
			entry.setPageFree(pos, false)
		}

		// THEN
		assert.True(t, entry.isAllUsed())
		commitMtr(t, mtr)
	})
}

func TestXdesEntryFirstFreePos(t *testing.T) {
	t.Run("初期化直後は最下位のページ位置 0 を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// WHEN
		got := entry.firstFreePos()

		// THEN
		assert.Equal(t, 0, got)
		commitMtr(t, mtr)
	})

	t.Run("先頭のページ位置を used にすると次の free ページ位置を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		entry.setPageFree(0, false)

		// WHEN
		got := entry.firstFreePos()

		// THEN
		assert.Equal(t, 1, got)
		commitMtr(t, mtr)
	})

	t.Run("最終ページ位置 255 のみ free の場合は 255 を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		for pos := range extentPageCount - 1 {
			entry.setPageFree(pos, false)
		}

		// WHEN
		got := entry.firstFreePos()

		// THEN
		assert.Equal(t, extentPageCount-1, got)
		commitMtr(t, mtr)
	})

	t.Run("byte 境界を跨ぐ位置 (8) が free の場合はその位置を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		for pos := range 8 {
			entry.setPageFree(pos, false)
		}

		// WHEN
		got := entry.firstFreePos()

		// THEN
		assert.Equal(t, 8, got)
		commitMtr(t, mtr)
	})

	t.Run("全ページ used の場合は -1 を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		for pos := range extentPageCount {
			entry.setPageFree(pos, false)
		}

		// WHEN
		got := entry.firstFreePos()

		// THEN
		assert.Equal(t, -1, got)
		commitMtr(t, mtr)
	})
}

func TestXdesEntryFreePageCount(t *testing.T) {
	t.Run("初期化直後は extent 内ページ数と一致する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// WHEN
		got := entry.freePageCount()

		// THEN
		assert.Equal(t, extentPageCount, got)
		commitMtr(t, mtr)
	})

	t.Run("used にした分だけ減る", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		entry.setPageFree(0, false)
		entry.setPageFree(5, false)
		entry.setPageFree(extentPageCount-1, false)

		// WHEN
		got := entry.freePageCount()

		// THEN
		assert.Equal(t, extentPageCount-3, got)
		commitMtr(t, mtr)
	})

	t.Run("全ページ used で 0", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()
		for pos := range extentPageCount {
			entry.setPageFree(pos, false)
		}

		// WHEN
		got := entry.freePageCount()

		// THEN
		assert.Equal(t, 0, got)
		commitMtr(t, mtr)
	})
}

func TestXdesEntryFlstNodeAddress(t *testing.T) {
	t.Run("エントリインデックスからボディ相対の node アドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry0 := writableEntry(t, mtr, 0)
		entry2 := writableEntry(t, mtr, 2)

		// WHEN
		addr0 := entry0.flstNodeAddress()
		addr2 := entry2.flstNodeAddress()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 0, Offset: 108 + 8}, addr0)
		assert.Equal(t, flst.Address{PageNumber: 0, Offset: 108 + 2*56 + 8}, addr2)
		commitMtr(t, mtr)
	})
}

func TestXdesEntryInitialize(t *testing.T) {
	t.Run("初期化後は FREE 状態で全ページ free、node は無効アドレス、予約領域はゼロ", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)

		// WHEN
		entry.initialize()

		// THEN
		assert.Equal(t, stateFree, entry.state())
		assert.True(t, entry.isAllFree())
		assert.False(t, entry.isAllUsed())
		body := entry.bufPage.Data().Body()
		assert.Equal(t, make([]byte, xdesFlstNodeOffset-xdesIdOffset), body[entry.offset()+xdesIdOffset:entry.offset()+xdesFlstNodeOffset])
		prev := flst.ReadAddress(body, entry.offset()+xdesFlstNodeOffset)
		next := flst.ReadAddress(body, entry.offset()+xdesFlstNodeOffset+6)
		assert.True(t, prev.IsInvalid())
		assert.True(t, next.IsInvalid())
		commitMtr(t, mtr)
	})

	t.Run("NOT_INITED 以外のエントリの初期化は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableEntry(t, mtr, 0)
		entry.initialize()

		// THEN
		assert.Panics(t, func() { entry.initialize() })
		commitMtr(t, mtr)
	})
}

// writableEntry は page 0 を書き込み用に取得し、index のエントリを返す
func writableEntry(t *testing.T, mtr *buffer.Mtr, index int) xdesEntry {
	t.Helper()
	bufPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
	if err != nil {
		t.Fatalf("PageForWrite に失敗: %v", err)
	}
	return xdesEntry{bufPage: bufPage, index: index}
}

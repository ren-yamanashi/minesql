package fsp

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInodeEntryOffset(t *testing.T) {
	t.Run("エントリインデックスからボディ相対オフセットを算出する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry0 := writableInodeEntry(t, mtr, 1, 0)
		entry3 := writableInodeEntry(t, mtr, 1, 3)

		// WHEN
		off0 := entry0.offset()
		off3 := entry3.offset()

		// THEN
		assert.Equal(t, 12, off0)
		assert.Equal(t, 12+3*576, off3)
		commitMtr(t, mtr)
	})
}

func TestInodeEntrySegId(t *testing.T) {
	t.Run("setSegId で書き込んだ segment id を読み取れる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.setSegId(42)

		// WHEN
		got := entry.segId()

		// THEN
		assert.Equal(t, uint64(42), got)
		commitMtr(t, mtr)
	})
}

func TestInodeEntrySetSegId(t *testing.T) {
	t.Run("書き込んだ segment id が segId で復元できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)

		// WHEN
		entry.setSegId(42)

		// THEN
		assert.Equal(t, uint64(42), entry.segId())
		commitMtr(t, mtr)
	})
}

func TestInodeEntryNotFullNUsed(t *testing.T) {
	t.Run("setNotFullNUsed で書き込んだ値を読み取れる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.setNotFullNUsed(7)

		// WHEN
		got := entry.notFullNUsed()

		// THEN
		assert.Equal(t, uint32(7), got)
		commitMtr(t, mtr)
	})
}

func TestInodeEntrySetNotFullNUsed(t *testing.T) {
	t.Run("書き込んだ値が notFullNUsed で復元できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)

		// WHEN
		entry.setNotFullNUsed(7)

		// THEN
		assert.Equal(t, uint32(7), entry.notFullNUsed())
		commitMtr(t, mtr)
	})
}

func TestInodeEntryFreeListBase(t *testing.T) {
	t.Run("エントリインデックスからボディ相対の FREE リスト base アドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 2)

		// WHEN
		got := entry.freeListBase()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 1, Offset: uint16(12 + 2*576 + 12)}, got)
		commitMtr(t, mtr)
	})
}

func TestInodeEntryNotFullListBase(t *testing.T) {
	t.Run("エントリインデックスからボディ相対の NOT_FULL リスト base アドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 2)

		// WHEN
		got := entry.notFullListBase()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 1, Offset: uint16(12 + 2*576 + 28)}, got)
		commitMtr(t, mtr)
	})
}

func TestInodeEntryFullListBase(t *testing.T) {
	t.Run("エントリインデックスからボディ相対の FULL リスト base アドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 2)

		// WHEN
		got := entry.fullListBase()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 1, Offset: uint16(12 + 2*576 + 44)}, got)
		commitMtr(t, mtr)
	})
}

func TestInodeEntryMagic(t *testing.T) {
	t.Run("setMagic で書き込んだシグネチャを読み取れる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.setMagic()

		// WHEN
		got := entry.magic()

		// THEN
		assert.Equal(t, [4]byte{'S', 'E', 'G', 'I'}, got)
		commitMtr(t, mtr)
	})
}

func TestInodeEntrySetMagic(t *testing.T) {
	t.Run("固定値 SEGI を書き込める", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)

		// WHEN
		entry.setMagic()

		// THEN
		assert.Equal(t, [4]byte{'S', 'E', 'G', 'I'}, entry.magic())
		commitMtr(t, mtr)
	})
}

func TestInodeEntryClearMagic(t *testing.T) {
	t.Run("setMagic 後の状態からゼロ埋めできる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.setMagic()

		// WHEN
		entry.clearMagic()

		// THEN
		assert.Equal(t, [4]byte{0, 0, 0, 0}, entry.magic())
		commitMtr(t, mtr)
	})
}

func TestInodeEntryFragSlot(t *testing.T) {
	t.Run("setFragSlot で書き込んだ PageNumber を読み取れる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.setFragSlot(0, page.PageNumber(100))
		entry.setFragSlot(inodeFragSlotCount-1, page.PageNumber(200))

		// WHEN
		got0 := entry.fragSlot(0)
		gotLast := entry.fragSlot(inodeFragSlotCount - 1)

		// THEN
		assert.Equal(t, page.PageNumber(100), got0)
		assert.Equal(t, page.PageNumber(200), gotLast)
		commitMtr(t, mtr)
	})

	t.Run("範囲外の index は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)

		// THEN
		assert.Panics(t, func() { entry.fragSlot(-1) })
		assert.Panics(t, func() { entry.fragSlot(inodeFragSlotCount) })
		commitMtr(t, mtr)
	})
}

func TestInodeEntrySetFragSlot(t *testing.T) {
	t.Run("境界位置 0 と 127 の PageNumber を書き換えられる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)

		// WHEN
		entry.setFragSlot(0, page.PageNumber(5))
		entry.setFragSlot(inodeFragSlotCount-1, page.PageNumber(99))

		// THEN
		assert.Equal(t, page.PageNumber(5), entry.fragSlot(0))
		assert.Equal(t, page.PageNumber(99), entry.fragSlot(inodeFragSlotCount-1))
		commitMtr(t, mtr)
	})

	t.Run("範囲外の index は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)

		// THEN
		assert.Panics(t, func() { entry.setFragSlot(inodeFragSlotCount, page.PageNumber(0)) })
		commitMtr(t, mtr)
	})
}

func TestInodeEntryFirstFreeFragSlot(t *testing.T) {
	t.Run("初期化直後は最小 index の 0 を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.initializeEntry(1)

		// WHEN
		got := entry.firstFreeFragSlot()

		// THEN
		assert.Equal(t, 0, got)
		commitMtr(t, mtr)
	})

	t.Run("先頭 slot に PageNumber を入れると次の未使用 index を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.initializeEntry(1)
		entry.setFragSlot(0, page.PageNumber(10))

		// WHEN
		got := entry.firstFreeFragSlot()

		// THEN
		assert.Equal(t, 1, got)
		commitMtr(t, mtr)
	})

	t.Run("全 slot が使用中のとき -1 を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.initializeEntry(1)
		for i := range inodeFragSlotCount {
			entry.setFragSlot(i, page.PageNumber(i+1))
		}

		// WHEN
		got := entry.firstFreeFragSlot()

		// THEN
		assert.Equal(t, -1, got)
		commitMtr(t, mtr)
	})
}

func TestInodeEntryLastUsedFragSlot(t *testing.T) {
	t.Run("slot 0 のみ使用中なら 0 を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.initializeEntry(1)
		entry.setFragSlot(0, page.PageNumber(10))

		// WHEN
		got := entry.lastUsedFragSlot()

		// THEN
		assert.Equal(t, 0, got)
		commitMtr(t, mtr)
	})

	t.Run("途中の slot のみ使用中なら最大 index を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.initializeEntry(1)
		entry.setFragSlot(0, page.PageNumber(10))
		entry.setFragSlot(3, page.PageNumber(20))
		entry.setFragSlot(5, page.PageNumber(30))

		// WHEN
		got := entry.lastUsedFragSlot()

		// THEN
		assert.Equal(t, 5, got)
		commitMtr(t, mtr)
	})

	t.Run("全 slot が未使用なら -1 を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.initializeEntry(1)

		// WHEN
		got := entry.lastUsedFragSlot()

		// THEN
		assert.Equal(t, -1, got)
		commitMtr(t, mtr)
	})
}

func TestInodeEntryFragPageCount(t *testing.T) {
	t.Run("初期化直後は 0 を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.initializeEntry(1)

		// WHEN
		got := entry.fragPageCount()

		// THEN
		assert.Equal(t, 0, got)
		commitMtr(t, mtr)
	})

	t.Run("途中まで slot を埋めた分だけ増える", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.initializeEntry(1)
		entry.setFragSlot(0, page.PageNumber(10))
		entry.setFragSlot(3, page.PageNumber(20))
		entry.setFragSlot(5, page.PageNumber(30))

		// WHEN
		got := entry.fragPageCount()

		// THEN
		assert.Equal(t, 3, got)
		commitMtr(t, mtr)
	})

	t.Run("全 slot を埋めると inodeFragSlotCount を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)
		entry.initializeEntry(1)
		for i := range inodeFragSlotCount {
			entry.setFragSlot(i, page.PageNumber(i+1))
		}

		// WHEN
		got := entry.fragPageCount()

		// THEN
		assert.Equal(t, inodeFragSlotCount, got)
		commitMtr(t, mtr)
	})
}

func TestInodeEntryAddress(t *testing.T) {
	t.Run("エントリインデックスからボディ相対のエントリアドレスを返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry0 := writableInodeEntry(t, mtr, 1, 0)
		entry3 := writableInodeEntry(t, mtr, 1, 3)

		// WHEN
		addr0 := entry0.address()
		addr3 := entry3.address()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 1, Offset: 12}, addr0)
		assert.Equal(t, flst.Address{PageNumber: 1, Offset: uint16(12 + 3*576)}, addr3)
		commitMtr(t, mtr)
	})
}

func TestInodeEntryInitializeEntry(t *testing.T) {
	t.Run("初期化後は segId / notFullNUsed / magic / frag array 全 slot が設定される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		entry := writableInodeEntry(t, mtr, 1, 0)

		// WHEN
		entry.initializeEntry(42)

		// THEN
		assert.Equal(t, uint64(42), entry.segId())
		assert.Equal(t, uint32(0), entry.notFullNUsed())
		assert.Equal(t, [4]byte{'S', 'E', 'G', 'I'}, entry.magic())
		for i := range inodeFragSlotCount {
			assert.Equal(t, page.MaxPageNumber, entry.fragSlot(i))
		}
		commitMtr(t, mtr)
	})
}

func TestInodeEntryAddressRoundTrip(t *testing.T) {
	t.Run("address() の値から loadInodeEntryByAddress で同じエントリを復元できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer commitMtr(t, mtr)
		entry := writableInodeEntry(t, mtr, 1, 3)

		// WHEN
		addr := entry.address()
		got, err := loadInodeEntryByAddress(mtr, testFileId, addr)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, 3, got.index)
		assert.Equal(t, page.PageNumber(1), got.bufPage.PageId().PageNumber())
	})
}

// writableInodeEntry は pn のページを書き込み用に取得し、index の inode エントリを返す
func writableInodeEntry(t *testing.T, mtr *buffer.Mtr, pn page.PageNumber, index int) inodeEntry {
	t.Helper()
	bufPage, err := mtr.PageForWrite(page.NewId(testFileId, pn))
	if err != nil {
		t.Fatalf("PageForWrite に失敗: %v", err)
	}
	return inodeEntry{bufPage: bufPage, index: index}
}

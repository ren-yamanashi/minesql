package fsp

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireExtent(t *testing.T) {
	t.Run("lease 成立時は FREE_FRAG 末尾を XDES_FSEG_FRAG にして NOT_FULL 末尾へ繋ぐ", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		fillUntilDescriptorExtent(t, bp, redoLog)
		fragBefore := readFragNUsed(t, bp)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		_, err = acquireExtent(mtr, testFileId, h, entry)
		require.NoError(t, err)
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		readEntry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		notFullLen, err := flst.Length(readMtr, testFileId, readEntry.notFullListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), notFullLen)
		freeLen, err := flst.Length(readMtr, testFileId, readEntry.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), freeLen)
		assert.Equal(t, uint32(1), readEntry.notFullNUsed())
		descrPage, err := readMtr.PageForRead(page.NewId(testFileId, descriptorPageStride))
		require.NoError(t, err)
		leasedEntry := xdesEntry{bufPage: descrPage, index: 0}
		assert.Equal(t, stateFsegFrag, leasedEntry.state())
		assert.Equal(t, readEntry.segId(), leasedEntry.segmentId())
		readHeaderPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		readH := header{bufPage: readHeaderPage}
		assert.Equal(t, fragBefore-1, readH.fragNUsed())
	})

	t.Run("lease 不成立時は空間から XDES_FSEG を確保して segment FREE リスト末尾へ繋ぐ", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		drainFreeFrag(t, bp, redoLog)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		_, err = acquireExtent(mtr, testFileId, h, entry)
		require.NoError(t, err)
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		readEntry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		freeLen, err := flst.Length(readMtr, testFileId, readEntry.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), freeLen)
		firstNode, err := flst.First(readMtr, testFileId, readEntry.freeListBase())
		require.NoError(t, err)
		acquired, err := loadEntryByNodeAddress(readMtr, testFileId, firstNode)
		require.NoError(t, err)
		assert.Equal(t, stateFseg, acquired.state())
		assert.Equal(t, readEntry.segId(), acquired.segmentId())
	})

	t.Run("reserved が閾値未満なら先読みは発動しない", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		drainFreeFrag(t, bp, redoLog)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		_, err = acquireExtent(mtr, testFileId, h, entry)
		require.NoError(t, err)
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		readEntry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		freeLen, err := flst.Length(readMtr, testFileId, readEntry.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), freeLen)
	})
}

func TestFillSegmentFreeList(t *testing.T) {
	t.Run("reserved が 40 extent 以上のとき FREE リストへ 4 extent 追加される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		attachExtentsToSegmentFull(t, bp, redoLog, entryAddr, segFillReservedExtents)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		reserved, _, err := segmentReservedPages(mtr, testFileId, entry)
		require.NoError(t, err)
		fillSegmentFreeList(mtr, testFileId, h, entry, reserved)
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		readEntry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		freeLen, err := flst.Length(readMtr, testFileId, readEntry.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(segFillAddExtents), freeLen)
	})

	t.Run("reserved が閾値未満なら何もしない", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		reserved, _, err := segmentReservedPages(mtr, testFileId, entry)
		require.NoError(t, err)
		fillSegmentFreeList(mtr, testFileId, h, entry, reserved)
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		readEntry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		freeLen, err := flst.Length(readMtr, testFileId, readEntry.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), freeLen)
	})

	t.Run("先読み中に空間の容量が枯渇しても error なく打ち切られる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		attachExtentsToSegmentFull(t, bp, redoLog, entryAddr, segFillReservedExtents)
		drainSpaceFreeList(t, bp, redoLog)
		setupMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		p0, err := setupMtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		header{bufPage: p0}.setFreeLimit(page.MaxPageNumber - page.PageNumber(extentPageCount) + 1)
		commitMtr(t, setupMtr)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		reserved, _, err := segmentReservedPages(mtr, testFileId, entry)
		require.NoError(t, err)
		fillSegmentFreeList(mtr, testFileId, h, entry, reserved)
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		readEntry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		freeLen, err := flst.Length(readMtr, testFileId, readEntry.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), freeLen)
	})
}

// fillUntilDescriptorExtent は freeLimit が 2 枚目の記述子ページを含む extent の直後まで進むよう fill を繰り返す
func fillUntilDescriptorExtent(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer) {
	t.Helper()
	target := page.PageNumber(descriptorPageStride + extentPageCount)
	for {
		readMtr := buffer.NewMtr(bp)
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		freeLimit := header{bufPage: bufPage}.freeLimit()
		readMtr.UnpinAll()
		if freeLimit >= target {
			return
		}
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, fill(mtr, testFileId))
		commitMtr(t, mtr)
	}
}

// drainSpaceFreeList は空間 FREE リストの extent を全て取り出して空にする
func drainSpaceFreeList(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer) {
	t.Helper()
	for {
		readMtr := buffer.NewMtr(bp)
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		length, err := flst.Length(readMtr, testFileId, h.freeListBase())
		require.NoError(t, err)
		readMtr.UnpinAll()
		if length == 0 {
			return
		}
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		mh := header{bufPage: headerPage}
		first, err := flst.First(mtr, testFileId, mh.freeListBase())
		require.NoError(t, err)
		require.NoError(t, flst.Remove(mtr, testFileId, mh.freeListBase(), first))
		commitMtr(t, mtr)
	}
}

// drainFreeFrag は空間 FREE_FRAG リストの extent を全て使い切って FULL_FRAG へ遷移させる
func drainFreeFrag(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer) {
	t.Helper()
	for {
		readMtr := buffer.NewMtr(bp)
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		length, err := flst.Length(readMtr, testFileId, h.freeFragListBase())
		require.NoError(t, err)
		readMtr.UnpinAll()
		if length == 0 {
			return
		}
		allocateOne(t, bp, redoLog)
	}
}

// attachExtentsToSegmentFull は segment の FULL リストへ count 個の extent を空間から獲得して繋ぐ
func attachExtentsToSegmentFull(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, entryAddr flst.Address, count int) {
	t.Helper()
	for range count {
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		x, err := allocateExtent(mtr, testFileId, h)
		require.NoError(t, err)
		x.setSegmentId(entry.segId())
		x.setState(stateFseg)
		for pos := range extentPageCount {
			x.setPageFree(pos, false)
		}
		require.NoError(t, flst.AddLast(mtr, testFileId, entry.fullListBase(), x.flstNodeAddress()))
		commitMtr(t, mtr)
	}
}

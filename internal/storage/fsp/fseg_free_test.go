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

func TestFreeSegmentPage(t *testing.T) {
	t.Run("frag ページの解放で対応 slot が無効化され空間の free bit が立つ", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fragPageId := allocateSegmentPageOne(t, bp, redoLog, headerAt)

		// WHEN
		freeSegmentPageOne(t, bp, redoLog, headerAt, fragPageId)

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, page.MaxPageNumber, entry.fragSlot(1))
		isFree, err := IsPageFree(readMtr, fragPageId)
		require.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("専有 extent 内の 1 ページ解放で NOT_FULL_N_USED が 1 減算される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fillFragArray(t, bp, redoLog, headerAt)
		extentPageIds := allocateSegmentPageN(t, bp, redoLog, headerAt, 3)

		// WHEN
		freeSegmentPageOne(t, bp, redoLog, headerAt, extentPageIds[1])

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, uint32(2), entry.notFullNUsed())
	})

	t.Run("FULL の extent 内の 1 ページ解放で FULL から NOT_FULL へ遷移し NOT_FULL_N_USED に extentPageCount-1 が計上される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fillFragArray(t, bp, redoLog, headerAt)
		extentPageIds := allocateSegmentPageN(t, bp, redoLog, headerAt, extentPageCount)

		// WHEN
		freeSegmentPageOne(t, bp, redoLog, headerAt, extentPageIds[10])

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		fullLen, err := flst.Length(readMtr, testFileId, entry.fullListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), fullLen)
		notFullLen, err := flst.Length(readMtr, testFileId, entry.notFullListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), notFullLen)
		assert.Equal(t, uint32(extentPageCount-1), entry.notFullNUsed())
	})

	t.Run("XDES_FSEG の全ページを解放すると extent が空間 FREE リストへ返却される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fillFragArray(t, bp, redoLog, headerAt)
		extentPageIds := allocateSegmentPageN(t, bp, redoLog, headerAt, extentPageCount)
		beforeFreeLen := spaceFreeListLength(t, bp)

		// WHEN
		for _, id := range extentPageIds {
			freeSegmentPageOne(t, bp, redoLog, headerAt, id)
		}

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		fullLen, err := flst.Length(readMtr, testFileId, entry.fullListBase())
		require.NoError(t, err)
		notFullLen, err := flst.Length(readMtr, testFileId, entry.notFullListBase())
		require.NoError(t, err)
		freeLen, err := flst.Length(readMtr, testFileId, entry.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), fullLen)
		assert.Equal(t, uint32(0), notFullLen)
		assert.Equal(t, uint32(0), freeLen)
		assert.Equal(t, uint32(0), entry.notFullNUsed())
		readPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		readH := header{bufPage: readPage}
		afterFreeLen, err := flst.Length(readMtr, testFileId, readH.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, beforeFreeLen+1, afterFreeLen)
	})

	t.Run("XDES_FSEG_FRAG の予約分以外を解放すると extent が空間 FREE_FRAG リストへ返却される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fillFragArray(t, bp, redoLog, headerAt)
		drainFreeFrag(t, bp, redoLog)
		fillUntilDescriptorExtent(t, bp, redoLog)
		leasedPageIds := allocateSegmentPageN(t, bp, redoLog, headerAt, extentPageCount-1)
		leasedDescrPage := page.PageNumber(descriptorPageStride)

		// WHEN
		for _, id := range leasedPageIds {
			freeSegmentPageOne(t, bp, redoLog, headerAt, id)
		}

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		readPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		readH := header{bufPage: readPage}
		last, err := flst.Last(readMtr, testFileId, readH.freeFragListBase())
		require.NoError(t, err)
		assert.Equal(t, leasedDescrPage, last.PageNumber)
		returned, err := loadEntryByNodeAddress(readMtr, testFileId, last)
		require.NoError(t, err)
		assert.Equal(t, stateFreeFrag, returned.state())
		assert.Equal(t, uint64(0), returned.segmentId())
		assert.False(t, returned.isPageFree(0))
	})

	t.Run("二重解放は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fragPageId := allocateSegmentPageOne(t, bp, redoLog, headerAt)
		freeSegmentPageOne(t, bp, redoLog, headerAt, fragPageId)

		// THEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer mtr.UnpinAll()
		assert.Panics(t, func() {
			_ = FreeSegmentPage(mtr, headerAt, fragPageId)
		})
	})

	t.Run("別 segment に属する専有 extent 内のページ解放は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		aPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAtA := flst.Address{PageNumber: aPageId.PageNumber(), Offset: 0}
		fillFragArray(t, bp, redoLog, headerAtA)
		aExtentPageIds := allocateSegmentPageN(t, bp, redoLog, headerAtA, 1)
		bPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAtB := flst.Address{PageNumber: bPageId.PageNumber(), Offset: 0}

		// THEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer mtr.UnpinAll()
		assert.Panics(t, func() {
			_ = FreeSegmentPage(mtr, headerAtB, aExtentPageIds[0])
		})
	})

	t.Run("frag 経路で解放したページは同じ segment の次の割り当てで再利用される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fragPageId := allocateSegmentPageOne(t, bp, redoLog, headerAt)

		// WHEN
		freeSegmentPageOne(t, bp, redoLog, headerAt, fragPageId)
		reused := allocateSegmentPageOne(t, bp, redoLog, headerAt)

		// THEN
		assert.Equal(t, fragPageId, reused)
	})
}

// freeSegmentPageOne は id のページを独立の mtr で FreeSegmentPage 経由で解放する
func freeSegmentPageOne(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, headerAt flst.Address, id page.Id) {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	require.NoError(t, FreeSegmentPage(mtr, headerAt, id))
	commitMtr(t, mtr)
}

func TestTouchAndWriteFreeSegmentPage(t *testing.T) {
	t.Run("frag ページを touch + write の 2 フェーズで解放できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fragPageId := allocateSegmentPageOne(t, bp, redoLog, headerAt)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		plan, err := TouchFreeSegmentPage(mtr, headerAt, fragPageId)
		require.NoError(t, err)
		WriteFreeSegmentPage(mtr, plan)
		commitMtr(t, mtr)

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, page.MaxPageNumber, entry.fragSlot(1))
		isFree, err := IsPageFree(readMtr, fragPageId)
		require.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("専有 extent 内ページを touch + write の 2 フェーズで解放できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fillFragArray(t, bp, redoLog, headerAt)
		extentPageIds := allocateSegmentPageN(t, bp, redoLog, headerAt, 3)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		plan, err := TouchFreeSegmentPage(mtr, headerAt, extentPageIds[1])
		require.NoError(t, err)
		WriteFreeSegmentPage(mtr, plan)
		commitMtr(t, mtr)

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, uint32(2), entry.notFullNUsed())
	})

	t.Run("FULL の extent 内ページの解放遷移を 2 フェーズで実行できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fillFragArray(t, bp, redoLog, headerAt)
		extentPageIds := allocateSegmentPageN(t, bp, redoLog, headerAt, extentPageCount)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		plan, err := TouchFreeSegmentPage(mtr, headerAt, extentPageIds[10])
		require.NoError(t, err)
		WriteFreeSegmentPage(mtr, plan)
		commitMtr(t, mtr)

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		fullLen, err := flst.Length(readMtr, testFileId, entry.fullListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), fullLen)
		notFullLen, err := flst.Length(readMtr, testFileId, entry.notFullListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), notFullLen)
		assert.Equal(t, uint32(extentPageCount-1), entry.notFullNUsed())
	})

	t.Run("Touch 後に Write を呼ばずに Commit しても解放されない", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fragPageId := allocateSegmentPageOne(t, bp, redoLog, headerAt)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		_, err := TouchFreeSegmentPage(mtr, headerAt, fragPageId)
		require.NoError(t, err)
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		isFree, err := IsPageFree(readMtr, fragPageId)
		require.NoError(t, err)
		assert.False(t, isFree)
	})

	t.Run("Touch と Write の合成は FreeSegmentPage と同じ最終状態になる", func(t *testing.T) {
		// GIVEN 2 つの同じ初期状態のプールを用意
		bp1, redoLog1 := setupTest(t)
		initFsp(t, bp1, redoLog1)
		firstPageId1 := createSegmentOne(t, bp1, redoLog1, 0)
		headerAt1 := flst.Address{PageNumber: firstPageId1.PageNumber(), Offset: 0}
		fillFragArray(t, bp1, redoLog1, headerAt1)
		extentIds1 := allocateSegmentPageN(t, bp1, redoLog1, headerAt1, extentPageCount)

		bp2, redoLog2 := setupTest(t)
		initFsp(t, bp2, redoLog2)
		firstPageId2 := createSegmentOne(t, bp2, redoLog2, 0)
		headerAt2 := flst.Address{PageNumber: firstPageId2.PageNumber(), Offset: 0}
		fillFragArray(t, bp2, redoLog2, headerAt2)
		extentIds2 := allocateSegmentPageN(t, bp2, redoLog2, headerAt2, extentPageCount)
		require.Equal(t, extentIds1, extentIds2)

		// WHEN 片方は FreeSegmentPage、もう片方は Touch + Write で解放
		freeSegmentPageOne(t, bp1, redoLog1, headerAt1, extentIds1[10])
		mtr2 := buffer.NewWriteMtr(bp2, lock.TrxId(1), redoLog2)
		plan, err := TouchFreeSegmentPage(mtr2, headerAt2, extentIds2[10])
		require.NoError(t, err)
		WriteFreeSegmentPage(mtr2, plan)
		commitMtr(t, mtr2)

		// THEN 2 つの entry の状態が一致
		readMtr1 := buffer.NewMtr(bp1)
		defer readMtr1.UnpinAll()
		readMtr2 := buffer.NewMtr(bp2)
		defer readMtr2.UnpinAll()
		entryAddr1 := readSegmentHeaderAt(t, bp1, firstPageId1, 0)
		entryAddr2 := readSegmentHeaderAt(t, bp2, firstPageId2, 0)
		entry1, err := loadInodeEntryByAddress(readMtr1, testFileId, entryAddr1)
		require.NoError(t, err)
		entry2, err := loadInodeEntryByAddress(readMtr2, testFileId, entryAddr2)
		require.NoError(t, err)
		assert.Equal(t, entry1.notFullNUsed(), entry2.notFullNUsed())
		fullLen1, _ := flst.Length(readMtr1, testFileId, entry1.fullListBase())
		fullLen2, _ := flst.Length(readMtr2, testFileId, entry2.fullListBase())
		assert.Equal(t, fullLen1, fullLen2)
		notFullLen1, _ := flst.Length(readMtr1, testFileId, entry1.notFullListBase())
		notFullLen2, _ := flst.Length(readMtr2, testFileId, entry2.notFullListBase())
		assert.Equal(t, notFullLen1, notFullLen2)
	})
}

// allocateSegmentPageN は count 個のページを 1 個ずつ独立の mtr で割り当て、返された PageId のスライスを返す
func allocateSegmentPageN(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, headerAt flst.Address, count int) []page.Id {
	t.Helper()
	ids := make([]page.Id, 0, count)
	for range count {
		ids = append(ids, allocateSegmentPageOne(t, bp, redoLog, headerAt))
	}
	return ids
}

// fillFragArray は frag array を満杯 (128 slot) になるまで割り当てる
func fillFragArray(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, headerAt flst.Address) {
	t.Helper()
	for range inodeFragSlotCount - 1 {
		allocateSegmentPageOne(t, bp, redoLog, headerAt)
	}
}

// spaceFreeListLength は空間の FREE リストの長さを返す
func spaceFreeListLength(t *testing.T, bp *buffer.Pool) uint32 {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	bufPage, err := mtr.PageForRead(page.NewId(testFileId, 0))
	require.NoError(t, err)
	h := header{bufPage: bufPage}
	length, err := flst.Length(mtr, testFileId, h.freeListBase())
	require.NoError(t, err)
	return length
}

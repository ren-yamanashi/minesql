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

func TestAllocateSegmentPage(t *testing.T) {
	t.Run("segment header 経由の呼び出しで frag 経路のページ番号が返る", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		got, err := AllocateSegmentPage(mtr, testFileId, headerAt)
		require.NoError(t, err)
		commitMtr(t, mtr)

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, firstPageId.PageNumber(), entry.fragSlot(0))
		assert.Equal(t, got.PageNumber(), entry.fragSlot(1))
	})

	t.Run("frag 経路で 128 ページまで確保できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}

		// WHEN
		for range inodeFragSlotCount - 1 {
			allocateSegmentPageOne(t, bp, redoLog, headerAt)
		}

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, inodeFragSlotCount, entry.fragPageCount())
	})

	t.Run("129 ページ目で extent を獲得し bitmap の最下位 free bit を割り当てる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		for range inodeFragSlotCount - 1 {
			allocateSegmentPageOne(t, bp, redoLog, headerAt)
		}

		// WHEN
		got := allocateSegmentPageOne(t, bp, redoLog, headerAt)

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, inodeFragSlotCount, entry.fragPageCount())
		notFullLen, err := flst.Length(readMtr, testFileId, entry.notFullListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), notFullLen)
		firstNode, err := flst.First(readMtr, testFileId, entry.notFullListBase())
		require.NoError(t, err)
		x, err := loadEntryByNodeAddress(readMtr, testFileId, firstNode)
		require.NoError(t, err)
		assert.Equal(t, extentFirstPageNumber(x), got.PageNumber())
		assert.Equal(t, uint32(1), entry.notFullNUsed())
	})

	t.Run("extent 満杯で NOT_FULL から FULL へ遷移し NOT_FULL_N_USED から extent ページ数を減算する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		for range inodeFragSlotCount - 1 {
			allocateSegmentPageOne(t, bp, redoLog, headerAt)
		}
		for range extentPageCount {
			allocateSegmentPageOne(t, bp, redoLog, headerAt)
		}

		// THEN
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		notFullLen, err := flst.Length(readMtr, testFileId, entry.notFullListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), notFullLen)
		fullLen, err := flst.Length(readMtr, testFileId, entry.fullListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), fullLen)
		assert.Equal(t, uint32(0), entry.notFullNUsed())
	})

	t.Run("FREE から NOT_FULL への遷移も反映される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		attachExtentsToSegmentFree(t, bp, redoLog, entryAddr, 1)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		_, err = allocatePageInSegment(mtr, testFileId, h, entry)
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
	})
}

func TestSegmentReservedPages(t *testing.T) {
	t.Run("frag ページのみの segment は frag 数を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		reserved, used, err := segmentReservedPages(mtr, testFileId, entry)
		require.NoError(t, err)

		// THEN
		assert.Equal(t, 1, reserved)
		assert.Equal(t, 1, used)
	})

	t.Run("FREE / NOT_FULL / FULL の extent 数と NOT_FULL_N_USED を合算する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		attachExtentsToSegmentFull(t, bp, redoLog, entryAddr, 2)
		attachExtentsToSegmentFree(t, bp, redoLog, entryAddr, 1)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
		require.NoError(t, err)
		reserved, used, err := segmentReservedPages(mtr, testFileId, entry)
		require.NoError(t, err)

		// THEN
		assert.Equal(t, 1+3*extentPageCount, reserved)
		assert.Equal(t, 1+2*extentPageCount, used)
	})
}

// allocateSegmentPageOne は独立の mtr で 1 ページ割り当て、返された PageId を返す
func allocateSegmentPageOne(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, headerAt flst.Address) page.Id {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	pageId, err := AllocateSegmentPage(mtr, testFileId, headerAt)
	require.NoError(t, err)
	commitMtr(t, mtr)
	return pageId
}

// attachExtentsToSegmentFree は segment の FREE リストへ count 個の extent を空間から獲得して繋ぐ
func attachExtentsToSegmentFree(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, entryAddr flst.Address, count int) {
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
		require.NoError(t, flst.AddLast(mtr, testFileId, entry.freeListBase(), x.flstNodeAddress()))
		commitMtr(t, mtr)
	}
}

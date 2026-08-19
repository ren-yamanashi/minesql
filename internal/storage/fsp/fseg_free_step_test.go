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

func TestFreeSegmentStep(t *testing.T) {
	t.Run("extent は FULL → NOT_FULL → FREE の順で解放される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		attachExtentsToSegmentFull(t, bp, redoLog, entryAddr, 1)
		attachExtentsToSegmentNotFull(t, bp, redoLog, entryAddr, 1)
		attachExtentsToSegmentFree(t, bp, redoLog, entryAddr, 1)

		// WHEN
		done1 := freeSegmentStepOne(t, bp, redoLog, headerAt)
		firstListsAfter := readSegmentListLengths(t, bp, entryAddr)
		done2 := freeSegmentStepOne(t, bp, redoLog, headerAt)
		secondListsAfter := readSegmentListLengths(t, bp, entryAddr)
		done3 := freeSegmentStepOne(t, bp, redoLog, headerAt)
		thirdListsAfter := readSegmentListLengths(t, bp, entryAddr)

		// THEN
		assert.False(t, done1)
		assert.False(t, done2)
		assert.False(t, done3)
		assert.Equal(t, segmentListLengths{full: 0, notFull: 1, free: 1}, firstListsAfter)
		assert.Equal(t, segmentListLengths{full: 0, notFull: 0, free: 1}, secondListsAfter)
		assert.Equal(t, segmentListLengths{full: 0, notFull: 0, free: 0}, thirdListsAfter)
	})

	t.Run("extent がなくなると frag array の末尾 slot から順に解放される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fillFragArray(t, bp, redoLog, headerAt)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		lastSlotPage := readFragSlot(t, bp, entryAddr, inodeFragSlotCount-1)

		// WHEN
		done := freeSegmentStepOne(t, bp, redoLog, headerAt)

		// THEN
		assert.False(t, done)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, page.MaxPageNumber, entry.fragSlot(inodeFragSlotCount-1))
		isFree, err := IsPageFree(readMtr, page.NewId(testFileId, lastSlotPage))
		require.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("frag 末尾 slot の解放で全 slot が空になった場合 inode スロットも同じ step で解放され true を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)

		// WHEN
		done := freeSegmentStepOne(t, bp, redoLog, headerAt)

		// THEN
		assert.True(t, done)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), entry.segId())
		assert.Equal(t, [4]byte{0, 0, 0, 0}, entry.magic())
	})

	t.Run("完了済みの segment header ページ (header 型) の再呼び出しは header の free bit で true を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		require.True(t, freeSegmentStepOne(t, bp, redoLog, headerAt))
		isFree, err := isHeaderPageFreeInReadMtr(t, bp, firstPageId.PageNumber())
		require.NoError(t, err)
		require.True(t, isFree)

		// WHEN
		done := freeSegmentStepOne(t, bp, redoLog, headerAt)

		// THEN
		assert.True(t, done)
	})

	t.Run("完了済みの segment header ページ (leaf 型) の再呼び出しは segId 0 で true を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		hostPageId := allocateOne(t, bp, redoLog)
		_, err := bp.AddPage(hostPageId)
		require.NoError(t, err)
		headerAt := flst.Address{PageNumber: hostPageId.PageNumber(), Offset: 200}
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, CreateSegmentAt(mtr, testFileId, headerAt))
		commitMtr(t, mtr)
		require.True(t, freeSegmentStepOne(t, bp, redoLog, headerAt))

		// WHEN
		done := freeSegmentStepOne(t, bp, redoLog, headerAt)

		// THEN
		assert.True(t, done)
	})

	t.Run("複数 step の反復で segment 全体が解放される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		headerAt := flst.Address{PageNumber: firstPageId.PageNumber(), Offset: 0}
		fillFragArray(t, bp, redoLog, headerAt)
		entryAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		attachExtentsToSegmentFull(t, bp, redoLog, entryAddr, 1)
		attachExtentsToSegmentNotFull(t, bp, redoLog, entryAddr, 1)

		// WHEN
		steps := 0
		for {
			done := freeSegmentStepOne(t, bp, redoLog, headerAt)
			steps++
			if done {
				break
			}
			if steps > inodeFragSlotCount+10 {
				t.Fatalf("解放が完了しない: %d step", steps)
			}
		}

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, entryAddr)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), entry.segId())
	})
}

// segmentListLengths は segment 側 3 リストの長さを保持する
type segmentListLengths struct {
	full, notFull, free uint32
}

// freeSegmentStepOne は独立の mtr で 1 step 実行し、完了フラグを返す
func freeSegmentStepOne(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, headerAt flst.Address) bool {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	done, err := FreeSegmentStep(mtr, testFileId, headerAt)
	require.NoError(t, err)
	commitMtr(t, mtr)
	return done
}

// readSegmentListLengths は entryAddr の segment の FULL / NOT_FULL / FREE リスト長を読み取る
func readSegmentListLengths(t *testing.T, bp *buffer.Pool, entryAddr flst.Address) segmentListLengths {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
	require.NoError(t, err)
	fullLen, err := flst.Length(mtr, testFileId, entry.fullListBase())
	require.NoError(t, err)
	notFullLen, err := flst.Length(mtr, testFileId, entry.notFullListBase())
	require.NoError(t, err)
	freeLen, err := flst.Length(mtr, testFileId, entry.freeListBase())
	require.NoError(t, err)
	return segmentListLengths{full: fullLen, notFull: notFullLen, free: freeLen}
}

// isHeaderPageFreeInReadMtr は pageNumber が free bit 上で free かを読み取り mtr で確認する
func isHeaderPageFreeInReadMtr(t *testing.T, bp *buffer.Pool, pageNumber page.PageNumber) (bool, error) {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	return IsPageFree(mtr, page.NewId(testFileId, pageNumber))
}

// readFragSlot は entryAddr の segment の frag slot i の PageNumber を読み取る
func readFragSlot(t *testing.T, bp *buffer.Pool, entryAddr flst.Address, i int) page.PageNumber {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	entry, err := loadInodeEntryByAddress(mtr, testFileId, entryAddr)
	require.NoError(t, err)
	return entry.fragSlot(i)
}

// attachExtentsToSegmentNotFull は segment の NOT_FULL リストへ count 個の一部使用中 extent を空間から獲得して繋ぐ
func attachExtentsToSegmentNotFull(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, entryAddr flst.Address, count int) {
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
		x.setPageFree(0, false)
		require.NoError(t, flst.AddLast(mtr, testFileId, entry.notFullListBase(), x.flstNodeAddress()))
		entry.setNotFullNUsed(entry.notFullNUsed() + 1)
		commitMtr(t, mtr)
	}
}

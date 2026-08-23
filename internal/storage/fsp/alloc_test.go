package fsp

import (
	"errors"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocatePage(t *testing.T) {
	t.Run("初期化直後の最初の割り当ては page 1 を返し以降 2 3 と続く", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(mtr, testFileId))
		commitMtr(t, mtr)

		// WHEN
		pageIds := allocateN(t, bp, redoLog, 3)

		// THEN
		assert.Equal(t, page.NewId(testFileId, 1), pageIds[0])
		assert.Equal(t, page.NewId(testFileId, 2), pageIds[1])
		assert.Equal(t, page.NewId(testFileId, 3), pageIds[2])
	})

	t.Run("page 0 は割り当て結果に現れない", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(mtr, testFileId))
		commitMtr(t, mtr)

		// WHEN
		pageIds := allocateN(t, bp, redoLog, extentPageCount)

		// THEN
		for _, pid := range pageIds {
			assert.NotEqual(t, page.PageNumber(0), pid.PageNumber())
		}
	})

	t.Run("extent 0 の割り当て可能 255 ページを使い切ると FULL_FRAG へ遷移し次の割り当ては次 extent の先頭になる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(mtr, testFileId))
		commitMtr(t, mtr)

		// WHEN
		pageIds := allocateN(t, bp, redoLog, extentPageCount-1)
		nextId := allocateOne(t, bp, redoLog)

		// THEN
		assert.Equal(t, page.NewId(testFileId, page.PageNumber(extentPageCount-1)), pageIds[extentPageCount-2])
		assert.Equal(t, page.NewId(testFileId, page.PageNumber(extentPageCount)), nextId)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		fullLen, err := flst.Length(readMtr, testFileId, h.fullFragListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), fullLen)
		assert.Equal(t, uint32(1), h.fragNUsed())
	})

	t.Run("FREE_FRAG と FREE が空でも fill が発動して割り当てが継続する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(mtr, testFileId))
		commitMtr(t, mtr)

		// WHEN
		allocateN(t, bp, redoLog, 5*extentPageCount+1)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		assert.Equal(t, page.PageNumber(9*extentPageCount), h.freeLimit())
		assert.Equal(t, uint32(9*extentPageCount), h.size())
	})

	t.Run("割り当て 5000 ページで予約ページ (page 0 / page 4096) を返さず重複もない", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(mtr, testFileId))
		commitMtr(t, mtr)

		// WHEN
		const count = 5000
		pageIds := allocateN(t, bp, redoLog, count)

		// THEN
		seen := make(map[page.PageNumber]bool, count)
		for _, pid := range pageIds {
			pn := pid.PageNumber()
			assert.NotEqual(t, page.PageNumber(0), pn)
			assert.NotEqual(t, page.PageNumber(descriptorPageStride), pn)
			assert.False(t, seen[pn], "重複した PageNumber: %d", pn)
			seen[pn] = true
		}
		assert.Len(t, seen, count)
	})

	t.Run("空間の容量が枯渇した場合 AllocatePage は書き込み前 errCapacityExhausted を返し redo に何も書かない", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		exhaustSpace(t, bp, redoLog)
		sizeBefore, err := redoLog.Size()
		require.NoError(t, err)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		_, allocErr := AllocatePage(mtr, testFileId)
		mtr.UnpinAll()

		// THEN
		assert.Error(t, allocErr)
		assert.True(t, errors.Is(allocErr, errCapacityExhausted))
		sizeAfter, err := redoLog.Size()
		require.NoError(t, err)
		assert.Equal(t, sizeBefore, sizeAfter)
	})

	t.Run("記述子ページ 2 枚目 (page 4096) の予約領域 108 バイトはゼロ", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(mtr, testFileId))
		commitMtr(t, mtr)

		// WHEN
		allocateN(t, bp, redoLog, 5000)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, descriptorPageStride))
		require.NoError(t, err)
		reserved := bufPage.Data().Body()[:headerSize]
		assert.Equal(t, make([]byte, headerSize), reserved)
	})
}

func TestAllocateExtent(t *testing.T) {
	t.Run("FREE リスト先頭の extent が取り出されリストから外れる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		fillMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, fill(fillMtr, testFileId))
		commitMtr(t, fillMtr)
		beforeMtr := buffer.NewMtr(bp)
		beforePage, err := beforeMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		beforeH := header{bufPage: beforePage}
		beforeLen, err := flst.Length(beforeMtr, testFileId, beforeH.freeListBase())
		require.NoError(t, err)
		beforeFirst, err := flst.First(beforeMtr, testFileId, beforeH.freeListBase())
		require.NoError(t, err)
		beforeMtr.UnpinAll()

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := allocateExtent(mtr, testFileId, h)
		require.NoError(t, err)
		commitMtr(t, mtr)

		// THEN
		assert.Equal(t, beforeFirst.PageNumber, entry.flstNodeAddress().PageNumber)
		assert.Equal(t, beforeFirst.Offset, entry.flstNodeAddress().Offset)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		readPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		readH := header{bufPage: readPage}
		afterLen, err := flst.Length(readMtr, testFileId, readH.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, beforeLen-1, afterLen)
	})

	t.Run("FREE 空時は fill で補充してから取り出す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: headerPage}
		entry, err := allocateExtent(mtr, testFileId, h)
		require.NoError(t, err)
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		readPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		readH := header{bufPage: readPage}
		assert.Equal(t, page.PageNumber(5*extentPageCount), readH.freeLimit())
		assert.Equal(t, page.PageNumber(0), entry.bufPage.PageId().PageNumber())
	})
}

func TestLoadEntryByNodeAddress(t *testing.T) {
	t.Run("整列した node アドレスから対応する index のエントリが復元される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer commitMtr(t, mtr)
		addr := flst.Address{PageNumber: 0, Offset: uint16(headerSize + 3*xdesEntrySize + xdesFlstNodeOffset)}

		// WHEN
		entry, err := loadEntryByNodeAddress(mtr, testFileId, addr)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, 3, entry.index)
		assert.Equal(t, page.PageNumber(0), entry.bufPage.PageId().PageNumber())
	})

	t.Run("範囲外の Offset は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer commitMtr(t, mtr)
		belowRange := flst.Address{PageNumber: 0, Offset: uint16(headerSize + xdesFlstNodeOffset - xdesEntrySize)}
		aboveRange := flst.Address{PageNumber: 0, Offset: uint16(headerSize + descriptorEntriesPerPage*xdesEntrySize + xdesFlstNodeOffset)}

		// THEN
		assert.Panics(t, func() { _, _ = loadEntryByNodeAddress(mtr, testFileId, belowRange) })
		assert.Panics(t, func() { _, _ = loadEntryByNodeAddress(mtr, testFileId, aboveRange) })
	})

	t.Run("範囲内 index に丸まる非整列の Offset は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 0)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer commitMtr(t, mtr)
		misaligned := flst.Address{PageNumber: 0, Offset: uint16(headerSize + xdesFlstNodeOffset + 1)}

		// THEN
		assert.Panics(t, func() { _, _ = loadEntryByNodeAddress(mtr, testFileId, misaligned) })
	})
}

// allocateN は count 個のページを 1 個ずつ独立の mtr で割り当て、返された PageId のスライスを返す
func allocateN(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, count int) []page.Id {
	t.Helper()
	ids := make([]page.Id, 0, count)
	for range count {
		ids = append(ids, allocateOne(t, bp, redoLog))
	}
	return ids
}

// allocateOne は 1 ページ割り当て、返された PageId を返す
func allocateOne(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer) page.Id {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	id, err := AllocatePage(mtr, testFileId)
	require.NoError(t, err)
	commitMtr(t, mtr)
	return id
}

// exhaustSpace は freeLimit を sentinel 直下に押し上げ、FREE / FREE_FRAG リストを空にして
// 以降の割り当てが必ず容量枯渇で失敗する状態にする
func exhaustSpace(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer) {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
	require.NoError(t, err)
	h := header{bufPage: headerPage}
	for {
		first, err := flst.First(mtr, testFileId, h.freeListBase())
		require.NoError(t, err)
		if first.IsInvalid() {
			break
		}
		require.NoError(t, flst.Remove(mtr, testFileId, h.freeListBase(), first))
	}
	for {
		first, err := flst.First(mtr, testFileId, h.freeFragListBase())
		require.NoError(t, err)
		if first.IsInvalid() {
			break
		}
		require.NoError(t, flst.Remove(mtr, testFileId, h.freeFragListBase(), first))
	}
	h.setFreeLimit(page.MaxPageNumber - page.PageNumber(extentPageCount) + 1)
	commitMtr(t, mtr)
}

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

func TestFreePage(t *testing.T) {
	t.Run("FREE_FRAG 内の解放で対象ページの free bit が立ち FRAG_N_USED が減算される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		pageIds := allocateN(t, bp, redoLog, 3)
		fragBefore := readFragNUsed(t, bp)

		// WHEN
		freeOne(t, bp, redoLog, pageIds[1])

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		entry := xdesEntry{bufPage: bufPage, index: 0}
		assert.Equal(t, stateFreeFrag, entry.state())
		assert.True(t, entry.isPageFree(int(pageIds[1].PageNumber())))
		assert.Equal(t, fragBefore-1, h.fragNUsed())
	})

	t.Run("FULL_FRAG 内の解放で FREE_FRAG へ遷移し FRAG_N_USED が 255 加算される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		pageIds := allocateN(t, bp, redoLog, extentPageCount-1)
		fragBefore := readFragNUsed(t, bp)
		require.Equal(t, uint32(0), fragBefore)

		// WHEN
		freeOne(t, bp, redoLog, pageIds[5])

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		entry := xdesEntry{bufPage: bufPage, index: 0}
		assert.Equal(t, stateFreeFrag, entry.state())
		assert.True(t, entry.isPageFree(int(pageIds[5].PageNumber())))
		assert.Equal(t, uint32(extentPageCount-1), h.fragNUsed())
		fullLen, err := flst.Length(readMtr, testFileId, h.fullFragListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), fullLen)
		freeFragLen, err := flst.Length(readMtr, testFileId, h.freeFragListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), freeFragLen)
	})

	t.Run("全ページが free に戻った extent は FREE_FRAG から FREE へ移動する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		allocateN(t, bp, redoLog, extentPageCount-1)
		targetId := allocateOne(t, bp, redoLog)
		require.Equal(t, page.PageNumber(extentPageCount), targetId.PageNumber())
		freeBefore, freeFragBefore := readListLengths(t, bp)

		// WHEN
		freeOne(t, bp, redoLog, targetId)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		entry := xdesEntry{bufPage: bufPage, index: 1}
		assert.Equal(t, stateFree, entry.state())
		assert.Equal(t, uint32(0), h.fragNUsed())
		freeLen, err := flst.Length(readMtr, testFileId, h.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, freeBefore+1, freeLen)
		freeFragLen, err := flst.Length(readMtr, testFileId, h.freeFragListBase())
		require.NoError(t, err)
		assert.Equal(t, freeFragBefore-1, freeFragLen)
	})

	t.Run("解放したページは次の割り当てで再利用される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		pageIds := allocateN(t, bp, redoLog, 3)
		target := pageIds[1]

		// WHEN
		freeOne(t, bp, redoLog, target)
		reused := allocateOne(t, bp, redoLog)

		// THEN
		assert.Equal(t, target, reused)
	})

	t.Run("解放前後で対象ページの中身は書き換わらない", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		target := allocateOne(t, bp, redoLog)
		_, err := bp.AddPage(target)
		require.NoError(t, err)
		payload := []byte("payload-preserved-across-free")
		writeMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		bufPage, err := writeMtr.PageForWrite(target)
		require.NoError(t, err)
		bufPage.WriteBodyAt(0, payload)
		commitMtr(t, writeMtr)

		// WHEN
		freeOne(t, bp, redoLog, target)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		freed, err := readMtr.PageForRead(target)
		require.NoError(t, err)
		assert.Equal(t, payload, freed.Data().Body()[:len(payload)])
	})

	t.Run("既に free の bit を持つページの解放は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		target := allocateOne(t, bp, redoLog)
		freeOne(t, bp, redoLog, target)

		// THEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer mtr.UnpinAll()
		assert.Panics(t, func() {
			_ = FreePage(mtr, target)
		})
	})

	t.Run("FREE 状態の extent 内のページ解放は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		_ = allocateOne(t, bp, redoLog)
		freeExtentPageId := page.NewId(testFileId, extentPageCount+5)

		// THEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer mtr.UnpinAll()
		assert.Panics(t, func() {
			_ = FreePage(mtr, freeExtentPageId)
		})
	})

	t.Run("フリーリミット以上のページ番号は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		_ = allocateOne(t, bp, redoLog)
		beyondLimit := page.NewId(testFileId, 5*extentPageCount)

		// THEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer mtr.UnpinAll()
		assert.Panics(t, func() {
			_ = FreePage(mtr, beyondLimit)
		})
	})
}

func TestIsPageFree(t *testing.T) {
	t.Run("使用中のページは false を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		target := allocateOne(t, bp, redoLog)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := IsPageFree(mtr, target)

		// THEN
		require.NoError(t, err)
		assert.False(t, got)
	})

	t.Run("解放済みのページは true を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		target := allocateOne(t, bp, redoLog)
		freeOne(t, bp, redoLog, target)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := IsPageFree(mtr, target)

		// THEN
		require.NoError(t, err)
		assert.True(t, got)
	})

	t.Run("フリーリミット以上のページ番号は true を返す", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		_ = allocateOne(t, bp, redoLog)
		beyondLimit := page.NewId(testFileId, 5*extentPageCount)

		// WHEN
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		got, err := IsPageFree(mtr, beyondLimit)

		// THEN
		require.NoError(t, err)
		assert.True(t, got)
	})
}

// initFsp は FSP ヘッダーを初期化し 1 つの Mtr でコミットする
func initFsp(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer) {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	require.NoError(t, InitHeader(mtr, testFileId))
	commitMtr(t, mtr)
}

// freeOne は id のページを独立の mtr で解放する
func freeOne(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, id page.Id) {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	require.NoError(t, FreePage(mtr, id))
	commitMtr(t, mtr)
}

// readFragNUsed はヘッダーから FRAG_N_USED を読み取る
func readFragNUsed(t *testing.T, bp *buffer.Pool) uint32 {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	bufPage, err := mtr.PageForRead(page.NewId(testFileId, 0))
	require.NoError(t, err)
	return header{bufPage: bufPage}.fragNUsed()
}

// readListLengths は FREE / FREE_FRAG リストの長さを読み取る
func readListLengths(t *testing.T, bp *buffer.Pool) (uint32, uint32) {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	bufPage, err := mtr.PageForRead(page.NewId(testFileId, 0))
	require.NoError(t, err)
	h := header{bufPage: bufPage}
	freeLen, err := flst.Length(mtr, testFileId, h.freeListBase())
	require.NoError(t, err)
	freeFragLen, err := flst.Length(mtr, testFileId, h.freeFragListBase())
	require.NoError(t, err)
	return freeLen, freeFragLen
}

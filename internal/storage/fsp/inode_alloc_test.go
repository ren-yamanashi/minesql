package fsp

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocateInodeEntry(t *testing.T) {
	t.Run("InitHeader 直後の初回確保で inode ページが page 1 に確保される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)

		// WHEN
		entry := allocateInodeOne(t, bp, redoLog)

		// THEN
		assert.Equal(t, page.PageNumber(1), entry.bufPage.PageId().PageNumber())
		assert.Equal(t, 0, entry.index)
	})

	t.Run("初回確保後の SEG_INODES_FREE は 1 件、SEG_INODES_FULL は空", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)

		// WHEN
		allocateInodeOne(t, bp, redoLog)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		h := header{}
		freeLen, err := flst.Length(readMtr, testFileId, h.segInodesFreeBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), freeLen)
		fullLen, err := flst.Length(readMtr, testFileId, h.segInodesFullBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), fullLen)
	})

	t.Run("7 スロット逐次確保で最後の確保時に SEG_INODES_FREE から SEG_INODES_FULL へ遷移する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)

		// WHEN
		entries := allocateInodeN(t, bp, redoLog, inodeEntriesPerPage)

		// THEN
		for i, e := range entries {
			assert.Equal(t, page.PageNumber(1), e.bufPage.PageId().PageNumber())
			assert.Equal(t, i, e.index)
		}
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		h := header{}
		freeLen, err := flst.Length(readMtr, testFileId, h.segInodesFreeBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), freeLen)
		fullLen, err := flst.Length(readMtr, testFileId, h.segInodesFullBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), fullLen)
	})

	t.Run("8 個目の確保で 2 枚目の inode ページが新規確保される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		allocateInodeN(t, bp, redoLog, inodeEntriesPerPage)

		// WHEN
		entry := allocateInodeOne(t, bp, redoLog)

		// THEN
		assert.NotEqual(t, page.PageNumber(1), entry.bufPage.PageId().PageNumber())
		assert.Equal(t, 0, entry.index)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		h := header{}
		freeLen, err := flst.Length(readMtr, testFileId, h.segInodesFreeBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), freeLen)
		fullLen, err := flst.Length(readMtr, testFileId, h.segInodesFullBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), fullLen)
	})

	t.Run("解放済みの常駐ページが inode ページとして再利用されても全スロットが未使用で始まる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		hostPageId := allocateOne(t, bp, redoLog)
		require.Equal(t, page.NewId(testFileId, 1), hostPageId)
		_, err := bp.AddPage(hostPageId)
		require.NoError(t, err)
		writeMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		bufPage, err := writeMtr.PageForWrite(hostPageId)
		require.NoError(t, err)
		junk := make([]byte, page.Size-page.HeaderSize)
		for i := range junk {
			junk[i] = 0xAB
		}
		bufPage.WriteBodyAt(0, junk)
		commitMtr(t, writeMtr)
		freeOne(t, bp, redoLog, hostPageId)

		// WHEN
		entry := allocateInodeOne(t, bp, redoLog)

		// THEN
		assert.Equal(t, page.PageNumber(1), entry.bufPage.PageId().PageNumber())
		assert.Equal(t, 0, entry.index)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		inodePage, err := readMtr.PageForRead(hostPageId)
		require.NoError(t, err)
		for i := 1; i < inodeEntriesPerPage; i++ {
			other := inodeEntry{bufPage: inodePage, index: i}
			assert.Equal(t, uint64(0), other.segId())
		}
		h := header{}
		freeLen, err := flst.Length(readMtr, testFileId, h.segInodesFreeBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), freeLen)
	})
}

func TestEnsureFreeInodePage(t *testing.T) {
	t.Run("AllocatePage 後の AddPage 失敗時に割り当てが補償解放されて free に戻る", func(t *testing.T) {
		// GIVEN
		bp, redoLog := newTinyPoolTest(t)
		initFsp(t, bp, redoLog)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		ensureErr := ensureFreeInodePage(mtr, testFileId, header{bufPage: headerPage})
		mtr.UnpinAll()

		// THEN
		require.Error(t, ensureErr)
		assert.ErrorIs(t, ensureErr, buffer.ErrAllPagesUnevictable)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		isFree, err := IsPageFree(readMtr, page.NewId(testFileId, 1))
		require.NoError(t, err)
		assert.True(t, isFree, "補償解放によって page 1 は free に戻っているはず")
	})
}

func TestLoadInodeEntryByAddress(t *testing.T) {
	t.Run("整列したアドレスから対応する index のエントリが復元される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer commitMtr(t, mtr)
		addr := flst.Address{PageNumber: 1, Offset: uint16(inodeArrOffset + 3*inodeEntrySize)}

		// WHEN
		entry, err := loadInodeEntryByAddress(mtr, testFileId, addr)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, 3, entry.index)
		assert.Equal(t, page.PageNumber(1), entry.bufPage.PageId().PageNumber())
	})

	t.Run("範囲外の Offset は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer commitMtr(t, mtr)
		belowRange := flst.Address{PageNumber: 1, Offset: uint16(inodeArrOffset - 1)}
		aboveRange := flst.Address{PageNumber: 1, Offset: uint16(inodeArrOffset + inodeEntriesPerPage*inodeEntrySize)}

		// THEN
		assert.Panics(t, func() { _, _ = loadInodeEntryByAddress(mtr, testFileId, belowRange) })
		assert.Panics(t, func() { _, _ = loadInodeEntryByAddress(mtr, testFileId, aboveRange) })
	})

	t.Run("範囲内 index に丸まる非整列の Offset は panic する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t, 1)
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer commitMtr(t, mtr)
		misaligned := flst.Address{PageNumber: 1, Offset: uint16(inodeArrOffset + 1)}

		// THEN
		assert.Panics(t, func() { _, _ = loadInodeEntryByAddress(mtr, testFileId, misaligned) })
	})
}

// allocateInodeOne は 1 つの inode スロットを確保し、そのエントリを返す
//   - 各呼び出しは独立の mtr で行う
func allocateInodeOne(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer) inodeEntry {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
	require.NoError(t, err)
	entry, err := allocateInodeEntry(mtr, testFileId, header{bufPage: headerPage})
	require.NoError(t, err)
	entry.initializeEntry(1)
	commitMtr(t, mtr)
	return entry
}

// allocateInodeN は count 個の inode スロットを逐次確保し、そのエントリを返す
func allocateInodeN(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, count int) []inodeEntry {
	t.Helper()
	entries := make([]inodeEntry, 0, count)
	for range count {
		entries = append(entries, allocateInodeOne(t, bp, redoLog))
	}
	return entries
}

// newTinyPoolTest は maxPages = 1 のバッファプールでテスト環境を用意する
//   - page 0 のみプールに収まる。以降の新規ページ AddPage は追い出し不可で ErrAllPagesUnevictable を返す
func newTinyPoolTest(t *testing.T) (*buffer.Pool, *redo.Buffer) {
	t.Helper()
	redoLog, err := redo.NewBuffer(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = redoLog.Close() })
	hf, err := file.NewHeapFile(filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp := buffer.NewPool(page.Size, redoLog, nil)
	bp.RegisterHeapFile(testFileId, hf)
	return bp, redoLog
}

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

func TestCreateSegment(t *testing.T) {
	t.Run("InitHeader 直後の初回呼び出しで inode ページ = page 1、最初のページ = page 2 になる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)

		// WHEN
		pageId := createSegmentOne(t, bp, redoLog, 0)

		// THEN
		assert.Equal(t, page.NewId(testFileId, 2), pageId)
	})

	t.Run("segId の採番は 1 から始まり呼び出しごとにインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)

		// WHEN
		firstPageId := createSegmentOne(t, bp, redoLog, 0)
		secondPageId := createSegmentOne(t, bp, redoLog, 0)

		// THEN
		firstAddr := readSegmentHeaderAt(t, bp, firstPageId, 0)
		secondAddr := readSegmentHeaderAt(t, bp, secondPageId, 0)
		require.Equal(t, page.PageNumber(1), firstAddr.PageNumber)
		require.Equal(t, page.PageNumber(1), secondAddr.PageNumber)
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		firstEntry, err := loadInodeEntryByAddress(readMtr, testFileId, firstAddr)
		require.NoError(t, err)
		secondEntry, err := loadInodeEntryByAddress(readMtr, testFileId, secondAddr)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstEntry.segId())
		assert.Equal(t, uint64(2), secondEntry.segId())
		headerPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		assert.Equal(t, uint64(3), header{bufPage: headerPage}.segId())
	})

	t.Run("frag slot 0 に最初のページ番号が記録される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)

		// WHEN
		pageId := createSegmentOne(t, bp, redoLog, 0)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		addr := readSegmentHeaderAtMtr(t, readMtr, pageId, 0)
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, addr)
		require.NoError(t, err)
		assert.Equal(t, pageId.PageNumber(), entry.fragSlot(0))
		assert.Equal(t, page.MaxPageNumber, entry.fragSlot(1))
	})

	t.Run("headerOffset に書かれた inode エントリアドレスが ReadSegmentHeader で読み取れる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		const headerOffset uint16 = 24

		// WHEN
		pageId := createSegmentOne(t, bp, redoLog, headerOffset)

		// THEN
		addr := readSegmentHeaderAt(t, bp, pageId, headerOffset)
		assert.Equal(t, flst.Address{PageNumber: 1, Offset: uint16(inodeArrOffset)}, addr)
	})

	t.Run("初期化された 3 リストは空リストになる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)

		// WHEN
		pageId := createSegmentOne(t, bp, redoLog, 0)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		addr := readSegmentHeaderAtMtr(t, readMtr, pageId, 0)
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, addr)
		require.NoError(t, err)
		for _, base := range []flst.Address{entry.freeListBase(), entry.notFullListBase(), entry.fullListBase()} {
			length, err := flst.Length(readMtr, testFileId, base)
			require.NoError(t, err)
			assert.Equal(t, uint32(0), length)
		}
		assert.Equal(t, uint32(0), entry.notFullNUsed())
		assert.Equal(t, [4]byte{'S', 'E', 'G', 'I'}, entry.magic())
	})
}

func TestCreateSegmentAt(t *testing.T) {
	t.Run("headerAt に inode エントリアドレスが書き込まれ、最初のページは割り当てられない", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		hostPageId := allocateOne(t, bp, redoLog)
		require.Equal(t, page.NewId(testFileId, 1), hostPageId)
		_, err := bp.AddPage(hostPageId)
		require.NoError(t, err)
		headerAt := flst.Address{PageNumber: hostPageId.PageNumber(), Offset: 100}

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, CreateSegmentAt(mtr, testFileId, headerAt))
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		addr, err := ReadSegmentHeader(readMtr, testFileId, headerAt)
		require.NoError(t, err)
		entry, err := loadInodeEntryByAddress(readMtr, testFileId, addr)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), entry.segId())
		assert.Equal(t, page.MaxPageNumber, entry.fragSlot(0))
		nextPageId := allocateOne(t, bp, redoLog)
		assert.Equal(t, page.NewId(testFileId, 3), nextPageId)
	})
}

func TestReadSegmentHeader(t *testing.T) {
	t.Run("CreateSegment で書き込んだアドレスが復元できる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		const headerOffset uint16 = 8

		// WHEN
		pageId := createSegmentOne(t, bp, redoLog, headerOffset)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		got, err := ReadSegmentHeader(readMtr, testFileId, flst.Address{PageNumber: pageId.PageNumber(), Offset: headerOffset})
		require.NoError(t, err)
		assert.Equal(t, flst.Address{PageNumber: 1, Offset: uint16(inodeArrOffset)}, got)
	})
}

// createSegmentOne は独立の mtr で CreateSegment を呼び、確保したページの Id を返す
func createSegmentOne(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, headerOffset uint16) page.Id {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	pageId, err := CreateSegment(mtr, testFileId, headerOffset)
	require.NoError(t, err)
	commitMtr(t, mtr)
	return pageId
}

// readSegmentHeaderAt は独立の read mtr で ReadSegmentHeader を呼び、値を返す
func readSegmentHeaderAt(t *testing.T, bp *buffer.Pool, pageId page.Id, headerOffset uint16) flst.Address {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	return readSegmentHeaderAtMtr(t, mtr, pageId, headerOffset)
}

// readSegmentHeaderAtMtr は与えられた mtr で ReadSegmentHeader を呼び、値を返す
func readSegmentHeaderAtMtr(t *testing.T, mtr *buffer.Mtr, pageId page.Id, headerOffset uint16) flst.Address {
	t.Helper()
	addr, err := ReadSegmentHeader(mtr, testFileId, flst.Address{PageNumber: pageId.PageNumber(), Offset: headerOffset})
	require.NoError(t, err)
	return addr
}

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

func TestFreeInodeEntry(t *testing.T) {
	t.Run("SEG_INODES_FULL の inode ページの 1 スロット解放で SEG_INODES_FREE へ移動する", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		entries := allocateInodeN(t, bp, redoLog, inodeEntriesPerPage)

		// WHEN
		freeInodeEntryOne(t, bp, redoLog, entries[0].bufPage.PageId().PageNumber(), 0)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		h := header{}
		fullLen, err := flst.Length(readMtr, testFileId, h.segInodesFullBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), fullLen)
		freeLen, err := flst.Length(readMtr, testFileId, h.segInodesFreeBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), freeLen)
	})

	t.Run("解放後の entry は segId 0 で magic がゼロ埋めされる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		entry := allocateInodeOne(t, bp, redoLog)

		// WHEN
		freeInodeEntryOne(t, bp, redoLog, entry.bufPage.PageId().PageNumber(), entry.index)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		inodePage, err := readMtr.PageForRead(page.NewId(testFileId, entry.bufPage.PageId().PageNumber()))
		require.NoError(t, err)
		freed := inodeEntry{bufPage: inodePage, index: entry.index}
		assert.Equal(t, uint64(0), freed.segId())
		assert.Equal(t, [4]byte{0, 0, 0, 0}, freed.magic())
	})

	t.Run("全スロットが未使用に戻ると inode ページ自体が空間へ解放される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		entry := allocateInodeOne(t, bp, redoLog)
		inodePageNumber := entry.bufPage.PageId().PageNumber()

		// WHEN
		freeInodeEntryOne(t, bp, redoLog, inodePageNumber, entry.index)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		h := header{}
		freeLen, err := flst.Length(readMtr, testFileId, h.segInodesFreeBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), freeLen)
		isFree, err := IsPageFree(readMtr, page.NewId(testFileId, inodePageNumber))
		require.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("解放した inode スロットは次の segment 作成で再利用される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initFsp(t, bp, redoLog)
		entries := allocateInodeN(t, bp, redoLog, 2)
		freeInodeEntryOne(t, bp, redoLog, entries[0].bufPage.PageId().PageNumber(), entries[0].index)

		// WHEN
		reused := allocateInodeOne(t, bp, redoLog)

		// THEN
		assert.Equal(t, entries[0].bufPage.PageId().PageNumber(), reused.bufPage.PageId().PageNumber())
		assert.Equal(t, entries[0].index, reused.index)
	})
}

// freeInodeEntryOne は 1 つの inode スロットを独立の mtr で freeInodeEntry で解放する
func freeInodeEntryOne(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, inodePageNumber page.PageNumber, index int) {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	headerPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
	require.NoError(t, err)
	h := header{bufPage: headerPage}
	inodePage, err := mtr.PageForWrite(page.NewId(testFileId, inodePageNumber))
	require.NoError(t, err)
	entry := inodeEntry{bufPage: inodePage, index: index}
	require.NoError(t, freeInodeEntry(mtr, testFileId, h, entry))
	commitMtr(t, mtr)
}

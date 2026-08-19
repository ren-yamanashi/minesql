package fsp

import (
	"errors"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFill(t *testing.T) {
	t.Run("初回 fill でフリーリミットが 5 extent 分進み SIZE も引き上がる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(initMtr, testFileId))
		commitMtr(t, initMtr)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, fill(mtr, testFileId))
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		assert.Equal(t, page.PageNumber(5*extentPageCount), h.freeLimit())
		assert.Equal(t, uint32(5*extentPageCount), h.size())
	})

	t.Run("初回 fill 後の FREE リストには 4 extent が積まれる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(initMtr, testFileId))
		commitMtr(t, initMtr)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, fill(mtr, testFileId))
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		h := header{}
		length, err := flst.Length(readMtr, testFileId, h.freeListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(freeAddExtents), length)
	})

	t.Run("初回 fill 後の FREE_FRAG リストには記述子 extent のみ 1 件積まれ FRAG_N_USED は 1 になる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(initMtr, testFileId))
		commitMtr(t, initMtr)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, fill(mtr, testFileId))
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		assert.Equal(t, uint32(1), h.fragNUsed())
		length, err := flst.Length(readMtr, testFileId, h.freeFragListBase())
		require.NoError(t, err)
		assert.Equal(t, uint32(1), length)
	})

	t.Run("記述子 extent は先頭ページのみ used で他のページは free のままとなる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(initMtr, testFileId))
		commitMtr(t, initMtr)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, fill(mtr, testFileId))
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		entry := xdesEntry{bufPage: bufPage, index: 0}
		assert.Equal(t, stateFreeFrag, entry.state())
		assert.False(t, entry.isPageFree(0))
		assert.True(t, entry.isPageFree(1))
		assert.True(t, entry.isPageFree(extentPageCount-1))
	})

	t.Run("2 枚目の記述子ページを跨ぐ fill で page 4096 が作成され予約領域 108 バイトはゼロのまま", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(initMtr, testFileId))
		commitMtr(t, initMtr)

		// WHEN
		for range 4 {
			mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
			require.NoError(t, fill(mtr, testFileId))
			commitMtr(t, mtr)
		}

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, descriptorPageStride))
		require.NoError(t, err)
		reserved := bufPage.Data().Body()[:headerSize]
		assert.Equal(t, make([]byte, headerSize), reserved)
		entry := xdesEntry{bufPage: bufPage, index: 0}
		assert.Equal(t, stateFreeFrag, entry.state())
		assert.False(t, entry.isPageFree(0))
	})

	t.Run("2 枚目の記述子ページを含む extent の FRAG_N_USED は 2 になる", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(initMtr, testFileId))
		commitMtr(t, initMtr)

		// WHEN
		for range 4 {
			mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
			require.NoError(t, fill(mtr, testFileId))
			commitMtr(t, mtr)
		}

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		require.NoError(t, err)
		h := header{bufPage: bufPage}
		assert.Equal(t, uint32(2), h.fragNUsed())
	})

	t.Run("フリーリミットが sentinel 近傍にあると extent 初期化前に容量枯渇 error が返る", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)
		initMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		require.NoError(t, InitHeader(initMtr, testFileId))
		commitMtr(t, initMtr)
		setupMtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		p0, err := setupMtr.PageForWrite(page.NewId(testFileId, 0))
		require.NoError(t, err)
		header{bufPage: p0}.setFreeLimit(page.MaxPageNumber - page.PageNumber(extentPageCount) + 1)
		commitMtr(t, setupMtr)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		defer mtr.UnpinAll()
		err = fill(mtr, testFileId)

		// THEN
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errCapacityExhausted))
	})
}

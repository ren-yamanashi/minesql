package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestFlushAllPagesFlushesRedoBeforeData(t *testing.T) {
	t.Run("データページ書き出し前に Redo がディスクへ flush される", func(t *testing.T) {
		// GIVEN
		rl := newTestRedoLog(t)
		bp := NewPool(page.Size*2, rl, nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.MarkModified()
		_, err = rl.AppendCommit(lock.TrxId(1))
		assert.NoError(t, err)
		assert.Equal(t, redo.Lsn(0), rl.FlushedLsn())

		// WHEN
		err = bp.FlushAllPages()

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, rl.FlushedLsn(), redo.Lsn(0))
	})
}

func TestFlushOldestPagesFlushesRedoBeforeData(t *testing.T) {
	t.Run("oldest フラッシュ前に Redo がディスクへ flush される", func(t *testing.T) {
		// GIVEN
		rl := newTestRedoLog(t)
		bp := NewPool(page.Size*2, rl, nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.MarkModified()
		_, err = rl.AppendCommit(lock.TrxId(2))
		assert.NoError(t, err)
		assert.Equal(t, redo.Lsn(0), rl.FlushedLsn())

		// WHEN
		err = bp.FlushOldestPages(1)

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, rl.FlushedLsn(), redo.Lsn(0))
	})
}

func TestFlushSinglePageFlushesRedoBeforeData(t *testing.T) {
	t.Run("追い出し救済の単発フラッシュ前に Redo がディスクへ flush される", func(t *testing.T) {
		// GIVEN
		rl := newTestRedoLog(t)
		bp := NewPool(page.Size*2, rl, nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p, err := bp.Page(pageId)
		assert.NoError(t, err)
		p.MarkModified()
		bp.Unpin(pageId)
		_, err = rl.AppendCommit(lock.TrxId(3))
		assert.NoError(t, err)
		assert.Equal(t, redo.Lsn(0), rl.FlushedLsn())

		// WHEN
		err = bp.flushSinglePage()

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, rl.FlushedLsn(), redo.Lsn(0))
	})

	t.Run("ダーティーページが存在しない場合は Redo flush しない", func(t *testing.T) {
		// GIVEN
		rl := newTestRedoLog(t)
		bp := NewPool(page.Size*2, rl, nil)
		_, err := rl.AppendCommit(lock.TrxId(4))
		assert.NoError(t, err)
		assert.Equal(t, redo.Lsn(0), rl.FlushedLsn())

		// WHEN
		err = bp.flushSinglePage()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, redo.Lsn(0), rl.FlushedLsn())
	})
}

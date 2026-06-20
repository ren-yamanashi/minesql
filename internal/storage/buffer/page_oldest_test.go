package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestMarkDirtyFromLsn(t *testing.T) {
	t.Run("初期状態の oldestModificationLsn は 0", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		// THEN
		assert.Equal(t, redo.Lsn(0), bp.oldestModificationLsn)
	})

	t.Run("初回呼び出しで oldestModificationLsn が設定される", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)

		// WHEN
		bp.markDirtyFromLsn(redo.Lsn(7))

		// THEN
		assert.Equal(t, redo.Lsn(7), bp.oldestModificationLsn)
	})

	t.Run("2 回目以降の呼び出しでは値が上書きされない (最初の LSN を保持)", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		bp.markDirtyFromLsn(redo.Lsn(7))

		// WHEN
		bp.markDirtyFromLsn(redo.Lsn(12))
		bp.markDirtyFromLsn(redo.Lsn(20))

		// THEN
		assert.Equal(t, redo.Lsn(7), bp.oldestModificationLsn)
	})

	t.Run("クリーン状態 (=0) に戻った後の呼び出しでは新しい LSN が設定される", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, newTestRedoLog(t), nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		bp.markDirtyFromLsn(redo.Lsn(7))
		bp.oldestModificationLsn = 0

		// WHEN
		bp.markDirtyFromLsn(redo.Lsn(15))

		// THEN
		assert.Equal(t, redo.Lsn(15), bp.oldestModificationLsn)
	})
}

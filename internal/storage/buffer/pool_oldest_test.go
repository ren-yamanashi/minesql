package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestForEachDirtyOldestLsn(t *testing.T) {
	t.Run("ダーティーページが無い場合はコールバックが呼ばれない", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size*4, newTestRedoLog(t), nil)

		// WHEN
		var collected []redo.Lsn
		pool.ForEachDirtyOldestLsn(func(lsn redo.Lsn) {
			collected = append(collected, lsn)
		})

		// THEN
		assert.Empty(t, collected)
	})

	t.Run("ダーティ化順に oldestModificationLsn が渡される", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size*4, newTestRedoLog(t), nil)
		ids := []page.Id{page.NewId(1, 0), page.NewId(1, 1), page.NewId(1, 2)}
		lsns := []redo.Lsn{10, 20, 30}
		for i, id := range ids {
			bp, err := pool.AddPage(id)
			assert.NoError(t, err)
			bp.markDirtyFromLsn(lsns[i])
			bp.MarkModified()
		}

		// WHEN
		var collected []redo.Lsn
		pool.ForEachDirtyOldestLsn(func(lsn redo.Lsn) {
			collected = append(collected, lsn)
		})

		// THEN
		assert.Equal(t, lsns, collected)
	})

	t.Run("同じページに対して markDirtyFromLsn が複数回呼ばれても最初の LSN を返す", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size*2, newTestRedoLog(t), nil)
		pageId := page.NewId(1, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		bp.markDirtyFromLsn(redo.Lsn(7))
		bp.MarkModified()
		bp.markDirtyFromLsn(redo.Lsn(20))
		bp.markDirtyFromLsn(redo.Lsn(30))

		// WHEN
		var collected []redo.Lsn
		pool.ForEachDirtyOldestLsn(func(lsn redo.Lsn) {
			collected = append(collected, lsn)
		})

		// THEN
		assert.Equal(t, []redo.Lsn{7}, collected)
	})
}

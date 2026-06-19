package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestFlushAndCleanResetsOldestModificationLsn(t *testing.T) {
	t.Run("フラッシュ後に isDirty=false かつ oldestModificationLsn=0 になる", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		p, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p.markDirtyFromLsn(redo.Lsn(5))
		p.MarkModified()

		// WHEN
		err = bp.FlushAllPages()

		// THEN
		assert.NoError(t, err)
		assert.False(t, p.isDirty)
		assert.Equal(t, redo.Lsn(0), p.oldestModificationLsn)
	})

	t.Run("並行書き込みで modifyCount が変化していた場合は oldestModificationLsn を維持する", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*2, nil)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := page.NewId(0, 0)
		p, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		p.markDirtyFromLsn(redo.Lsn(5))
		p.MarkModified()

		tasks, err := bp.collectAllFlushTasks()
		assert.NoError(t, err)
		p.MarkModified()
		for _, task := range tasks {
			task.bufPage.latch.LockShared()
			err = bp.flushAndClean(task)
			assert.NoError(t, err)
		}

		// THEN
		assert.True(t, p.isDirty)
		assert.Equal(t, redo.Lsn(5), p.oldestModificationLsn)
	})
}

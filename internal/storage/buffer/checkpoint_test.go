package buffer

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/log"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCheckpointExecute(t *testing.T) {
	t.Run("ダーティーページがある場合、最小 Page LSN - 1 がチェックポイント LSN になる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(10, rl)

		disk, err := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(page.FileId(1), disk)

		// ページを作成してダーティーにし、Page LSN を設定
		pageId, _ := bp.AllocatePageId(page.FileId(1))
		_ = bp.AddPage(pageId)
		data, _ := bp.GetWritePageData(pageId)
		pg := page.NewPage(data)
		binary.BigEndian.PutUint32(pg.Header, uint32(5)) // Page LSN = 5

		// REDO ログにレコードを記録してフラッシュ
		rl.AppendPageCopy(1, pageId, data)
		err = rl.Flush()
		assert.NoError(t, err)

		cp := NewCheckpoint(bp, rl)

		// WHEN
		err = cp.Execute()
		assert.NoError(t, err)

		// THEN: checkpointLSN = 5 - 1 = 4
		assert.Equal(t, log.LSN(4), rl.CheckpointLSN())
	})

	t.Run("ダーティーページがない場合、FlushedLSN がチェックポイント LSN になる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(10, rl)

		// REDO ログにレコードを記録してフラッシュ (ダーティーページなし)
		rl.AppendCommit(1)
		err = rl.Flush()
		assert.NoError(t, err)
		flushedLSN := rl.FlushedLSN()

		cp := NewCheckpoint(bp, rl)

		// WHEN
		err = cp.Execute()
		assert.NoError(t, err)

		// THEN: checkpointLSN = FlushedLSN
		assert.Equal(t, flushedLSN, rl.CheckpointLSN())
	})

	t.Run("Execute 後に古い REDO レコードが切り詰められる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := NewBufferPool(10, rl)

		disk, err := file.NewDisk(page.FileId(1), filepath.Join(tmpdir, "test.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(page.FileId(1), disk)

		// 3 ページ分の REDO レコードを記録
		pageId1, _ := bp.AllocatePageId(page.FileId(1))
		_ = bp.AddPage(pageId1)
		data1, _ := bp.GetWritePageData(pageId1)
		pg1 := page.NewPage(data1)
		binary.BigEndian.PutUint32(pg1.Header, uint32(1))
		rl.AppendPageCopy(1, pageId1, data1) // LSN=1

		pageId2, _ := bp.AllocatePageId(page.FileId(1))
		_ = bp.AddPage(pageId2)
		data2, _ := bp.GetWritePageData(pageId2)
		pg2 := page.NewPage(data2)
		binary.BigEndian.PutUint32(pg2.Header, uint32(2))
		rl.AppendPageCopy(1, pageId2, data2) // LSN=2

		rl.AppendCommit(1) // LSN=3
		err = rl.Flush()
		assert.NoError(t, err)

		// pageId1 をフラッシュしてクリーンにする (フラッシュリストに pageId2 だけ残る)
		err = bp.FlushOldestPages(1)
		assert.NoError(t, err)

		recordsBefore, _ := rl.ReadAll()

		cp := NewCheckpoint(bp, rl)

		// WHEN
		err = cp.Execute()
		assert.NoError(t, err)

		// THEN: 古い REDO レコードが切り詰められている
		recordsAfter, _ := rl.ReadAll()
		assert.Less(t, len(recordsAfter), len(recordsBefore))
	})
}

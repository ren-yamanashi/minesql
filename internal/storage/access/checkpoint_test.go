package access

import (
	"encoding/binary"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestNewCheckpoint(t *testing.T) {
	t.Run("Checkpoint を生成できる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)

		// WHEN
		cp := NewCheckpoint(env.bp, env.redoLog)

		// THEN
		assert.NotNil(t, cp)
	})
}

func TestCheckpointExecute(t *testing.T) {
	t.Run("ダーティーページがない場合 FlushedLsn がチェックポイント LSN になる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		env.redoLog.AppendCommit(lock.TrxId(1))
		_ = env.redoLog.Flush()
		cp := NewCheckpoint(env.bp, env.redoLog)

		// WHEN
		err := cp.Execute()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, env.redoLog.FlushedLsn(), env.redoLog.CheckpointLsn())
	})

	t.Run("ダーティーページがある場合 最小 Page LSN - 1 がチェックポイント LSN になる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		// セットアップで作られたダーティーページをクリア
		_ = env.bp.FlushAllPages()

		env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		env.redoLog.AppendCommit(lock.TrxId(2)) // LSN=2
		env.redoLog.AppendCommit(lock.TrxId(3)) // LSN=3
		_ = env.redoLog.Flush()

		// ダーティーページを作り Page LSN=3 を設定
		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		writePage, err := env.bp.PageForWrite(pgId)
		assert.NoError(t, err)
		binary.BigEndian.PutUint32(writePage.Data().Header, 3)

		cp := NewCheckpoint(env.bp, env.redoLog)

		// WHEN
		err = cp.Execute()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, redo.Lsn(2), env.redoLog.CheckpointLsn())
	})

	t.Run("チェックポイント LSN 以前の Redo レコードが切り詰められる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		env.redoLog.AppendCommit(lock.TrxId(2)) // LSN=2
		env.redoLog.AppendCommit(lock.TrxId(3)) // LSN=3
		_ = env.redoLog.Flush()

		// Page LSN=2 のダーティーページ → チェックポイント LSN = 1
		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		writePage, err := env.bp.PageForWrite(pgId)
		assert.NoError(t, err)
		binary.BigEndian.PutUint32(writePage.Data().Header, 2)

		cp := NewCheckpoint(env.bp, env.redoLog)

		// WHEN
		err = cp.Execute()

		// THEN
		assert.NoError(t, err)
		// LSN=1 が切り詰められ、LSN=2, 3 が残る
		records, err := env.redoLog.ReadAll()
		assert.NoError(t, err)
		assert.Len(t, records, 2)
		assert.Equal(t, redo.Lsn(2), records[0].Lsn())
		assert.Equal(t, redo.Lsn(3), records[1].Lsn())
	})

	t.Run("複数のダーティーページがある場合 最小の Page LSN が使われる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		env.redoLog.AppendCommit(lock.TrxId(2)) // LSN=2
		_ = env.redoLog.Flush()

		// Page LSN=5 のダーティーページ
		pgId1 := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId1)
		writePage1, err := env.bp.PageForWrite(pgId1)
		assert.NoError(t, err)
		binary.BigEndian.PutUint32(writePage1.Data().Header, 5)

		// Page LSN=2 のダーティーページ (こちらが最小)
		pgId2 := page.NewId(page.FileId(2), 1)
		_, _ = env.bp.AddPage(pgId2)
		writePage2, err := env.bp.PageForWrite(pgId2)
		assert.NoError(t, err)
		binary.BigEndian.PutUint32(writePage2.Data().Header, 2)

		cp := NewCheckpoint(env.bp, env.redoLog)

		// WHEN
		err = cp.Execute()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, redo.Lsn(1), env.redoLog.CheckpointLsn())
	})

	t.Run("Redo ログが空でもエラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		cp := NewCheckpoint(env.bp, env.redoLog)

		// WHEN
		err := cp.Execute()

		// THEN
		assert.NoError(t, err)
	})
}

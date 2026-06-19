package access

import (
	"encoding/binary"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
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
		_, _ = env.redoLog.AppendCommit(lock.TrxId(1))
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

		_, _ = env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = env.redoLog.AppendCommit(lock.TrxId(2)) // LSN=2
		_, _ = env.redoLog.AppendCommit(lock.TrxId(3)) // LSN=3
		_ = env.redoLog.Flush()

		// ダーティーページを作り Page LSN=3 を設定
		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		writePage, err := env.bp.Page(pgId)
		assert.NoError(t, err)
		var lsnBuf [4]byte
		binary.BigEndian.PutUint32(lsnBuf[:], 3)
		writePage.WriteHeaderAt(0, lsnBuf[:])

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

		_, _ = env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = env.redoLog.AppendCommit(lock.TrxId(2)) // LSN=2
		_, _ = env.redoLog.AppendCommit(lock.TrxId(3)) // LSN=3
		_ = env.redoLog.Flush()

		// Page LSN=2 のダーティーページ → チェックポイント LSN = 1
		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		writePage, err := env.bp.Page(pgId)
		assert.NoError(t, err)
		var lsnBuf [4]byte
		binary.BigEndian.PutUint32(lsnBuf[:], 2)
		writePage.WriteHeaderAt(0, lsnBuf[:])

		cp := NewCheckpoint(env.bp, env.redoLog)

		// WHEN
		err = cp.Execute()

		// THEN
		assert.NoError(t, err)
		// LSN=1 が切り詰められ、LSN=2, 3 が残る
		records, err := env.redoLog.ReadFrom(redo.Lsn(0))
		assert.NoError(t, err)
		assert.Len(t, records, 2)
		assert.Equal(t, redo.Lsn(2), records[0].Lsn())
		assert.Equal(t, redo.Lsn(3), records[1].Lsn())
	})

	t.Run("複数のダーティーページがある場合 最小の Page LSN が使われる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		_, _ = env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = env.redoLog.AppendCommit(lock.TrxId(2)) // LSN=2
		_ = env.redoLog.Flush()

		// Page LSN=5 のダーティーページ
		pgId1 := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId1)
		writePage1, err := env.bp.Page(pgId1)
		assert.NoError(t, err)
		var lsnBuf1 [4]byte
		binary.BigEndian.PutUint32(lsnBuf1[:], 5)
		writePage1.WriteHeaderAt(0, lsnBuf1[:])

		// Page LSN=2 のダーティーページ (こちらが最小)
		pgId2 := page.NewId(page.FileId(2), 1)
		_, _ = env.bp.AddPage(pgId2)
		writePage2, err := env.bp.Page(pgId2)
		assert.NoError(t, err)
		var lsnBuf2 [4]byte
		binary.BigEndian.PutUint32(lsnBuf2[:], 2)
		writePage2.WriteHeaderAt(0, lsnBuf2[:])

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

	t.Run("実 Insert で生じたダーティーページの最小 Page LSN - 1 がチェックポイント LSN になる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)
		flushBaseline(t, env)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
		assert.NoError(t, err)
		trx := env.trxMgr.Begin()
		err = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trx)
		assert.NoError(t, err)

		minLsn := dirtyMinPageLsn(env.bp)
		assert.NotEqual(t, redo.Lsn(0), minLsn)

		// WHEN
		cp := NewCheckpoint(env.bp, env.redoLog)
		err = cp.Execute()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, minLsn-1, env.redoLog.CheckpointLsn())
	})
}

func TestCheckpointExecuteWithFuzzyFlushAndRecovery(t *testing.T) {
	t.Run("一部フラッシュ → Checkpoint で前進 → クラッシュ → Recovery で残りが復元される", func(t *testing.T) {
		// FIXME(7c): 7c で oldest_modification LSN を導入した後に有効化する
		// 現状の Checkpoint は Page LSN (newest_modification 相当) を minPageLsn として使うため、
		// 同一ページが繰り返し変更されると checkpointLsn が前進しすぎて Redo が truncate され、
		// 未フラッシュページのデータがロスする
		t.Skip("FIXME(7c): oldest_modification LSN 導入後に有効化")

		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)
		flushBaseline(t, env)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
		assert.NoError(t, err)
		for _, row := range [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
			{"3", "Carol", "carol@example.com"},
		} {
			trx := env.trxMgr.Begin()
			err = table.Insert(trx, []string{"id", "name", "email"}, row)
			assert.NoError(t, err)
			err = env.trxMgr.Commit(trx)
			assert.NoError(t, err)
		}
		assert.NoError(t, env.redoLog.Flush())
		flushListBefore := env.bp.FlushListPageCount()
		assert.Greater(t, flushListBefore, 1)
		checkpointBefore := env.redoLog.CheckpointLsn()

		// WHEN
		assert.NoError(t, env.bp.FlushOldestPages(1))
		assert.Greater(t, flushListBefore, env.bp.FlushListPageCount())
		cp := NewCheckpoint(env.bp, env.redoLog)
		assert.NoError(t, cp.Execute())

		// THEN
		assert.Greater(t, env.redoLog.CheckpointLsn(), checkpointBefore)
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId())
		assert.NoError(t, r.Execute())

		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		mtr := buffer.NewMtr(env2.bp)
		defer mtr.UnpinAll()
		iter, err := table2.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		var ids []string
		for {
			rec, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			ids = append(ids, rec.values[0])
		}
		assert.Equal(t, []string{"1", "2", "3"}, ids)
	})
}

// dirtyMinPageLsn はバッファプール内ダーティーページの最小 Page LSN を取得する
func dirtyMinPageLsn(bp *buffer.Pool) redo.Lsn {
	var minLsn redo.Lsn
	first := true
	bp.ForEachDirtyPage(func(pg *page.Page) {
		lsn := redo.Lsn(binary.BigEndian.Uint32(pg.Header()))
		if first || lsn < minLsn {
			minLsn = lsn
			first = false
		}
	})
	return minLsn
}

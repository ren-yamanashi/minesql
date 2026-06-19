package access

import (
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

	t.Run("ダーティーページがある場合 最小ダーティ化開始 LSN - 1 がチェックポイント LSN になる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		_, _ = env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = env.redoLog.AppendCommit(lock.TrxId(2)) // LSN=2
		_, _ = env.redoLog.AppendCommit(lock.TrxId(3)) // LSN=3
		_ = env.redoLog.Flush()

		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		mtr := buffer.NewWriteMtr(env.bp, lock.TrxId(1), env.redoLog)
		bufPage, err := mtr.PageForWrite(pgId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{1})
		assert.NoError(t, mtr.Commit())
		mtrStartLsn := firstMtrStartLsn(t, env.redoLog)

		cp := NewCheckpoint(env.bp, env.redoLog)

		// WHEN
		err = cp.Execute()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, mtrStartLsn-1, env.redoLog.CheckpointLsn())
	})

	t.Run("チェックポイント LSN 以前の Redo レコードが切り詰められる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		_, _ = env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		_ = env.redoLog.Flush()

		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		mtr := buffer.NewWriteMtr(env.bp, lock.TrxId(1), env.redoLog)
		bufPage, err := mtr.PageForWrite(pgId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{1})
		assert.NoError(t, mtr.Commit())
		mtrStartLsn := firstMtrStartLsn(t, env.redoLog)

		cp := NewCheckpoint(env.bp, env.redoLog)

		// WHEN
		err = cp.Execute()

		// THEN
		assert.NoError(t, err)
		// checkpointLsn = mtrStartLsn - 1 以前のレコードが消える
		records, err := env.redoLog.ReadFrom(redo.Lsn(0))
		assert.NoError(t, err)
		for _, r := range records {
			assert.Greater(t, r.Lsn(), mtrStartLsn-1)
		}
	})

	t.Run("複数のダーティーページがある場合 最小のダーティ化開始 LSN が使われる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		pgId1 := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId1)
		mtr1 := buffer.NewWriteMtr(env.bp, lock.TrxId(1), env.redoLog)
		bufPage1, err := mtr1.PageForWrite(pgId1)
		assert.NoError(t, err)
		bufPage1.WriteBodyAt(0, []byte{1})
		assert.NoError(t, mtr1.Commit())
		firstMtrStart := firstMtrStartLsn(t, env.redoLog)

		pgId2 := page.NewId(page.FileId(2), 1)
		_, _ = env.bp.AddPage(pgId2)
		mtr2 := buffer.NewWriteMtr(env.bp, lock.TrxId(1), env.redoLog)
		bufPage2, err := mtr2.PageForWrite(pgId2)
		assert.NoError(t, err)
		bufPage2.WriteBodyAt(0, []byte{2})
		assert.NoError(t, mtr2.Commit())

		cp := NewCheckpoint(env.bp, env.redoLog)

		// WHEN
		err = cp.Execute()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, firstMtrStart-1, env.redoLog.CheckpointLsn())
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

	t.Run("実 Insert で生じたダーティーページの最小ダーティ化開始 LSN - 1 がチェックポイント LSN になる", func(t *testing.T) {
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

		minLsn := dirtyMinOldestLsn(env.bp)
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
		// FIXME(7d): 7d で以下の 2 点を解決した後に有効化する。
		// (1) 現シナリオは同じテーブルへの 3 件 Insert で同一 leaf / undo / secondary leaf
		//     ページを繰り返し変更するため、3 ページとも最初の MtrStart LSN を oldest として
		//     共有してしまい、FlushOldestPages(1) しても残ページの oldest が変わらず
		//     checkpointLsn が前進しない。複数の独立した mtr で異なるページを変更する
		//     シナリオへ再設計する必要がある (例: 別テーブルを使う / テーブル作成と Insert を
		//     分離する)。
		// (2) DDL 経路 (CreateCatalog / persistScalar / registerTableMeta / btree.CreateTree /
		//     undo.NewManager) は読み取り専用 Mtr で書き込みを行い、Redo に記録されない。
		//     Recovery でカタログヘッダーやメタテーブルの変更が復元されないデータロス問題が
		//     残る。NewWriteMtr 経由に置換する必要がある (C2 を緑にする条件ではないが、独立した
		//     深刻問題のため 7d でまとめて対処)。
		t.Skip("FIXME(7d): C2 シナリオ再設計 + DDL の Redo 化 完了後に有効化")

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

// dirtyMinOldestLsn はバッファプール内ダーティーページの最小ダーティ化開始 LSN を取得する
func dirtyMinOldestLsn(bp *buffer.Pool) redo.Lsn {
	var minLsn redo.Lsn
	first := true
	bp.ForEachDirtyOldestLsn(func(lsn redo.Lsn) {
		if first || lsn < minLsn {
			minLsn = lsn
			first = false
		}
	})
	return minLsn
}

// firstMtrStartLsn は Redo ログの先頭の MtrStart レコードの LSN を取得する
func firstMtrStartLsn(t *testing.T, rl *redo.Buffer) redo.Lsn {
	t.Helper()
	assert.NoError(t, rl.Flush())
	records, err := rl.ReadFrom(redo.Lsn(0))
	assert.NoError(t, err)
	for _, r := range records {
		if r.Type() == redo.RecordTypeMtrStart {
			return r.Lsn()
		}
	}
	t.Fatalf("MtrStart レコードが見つからない")
	return redo.Lsn(0)
}

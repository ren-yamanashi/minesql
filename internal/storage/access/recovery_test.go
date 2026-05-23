package access

import (
	"os"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestNewRecovery(t *testing.T) {
	t.Run("Recovery を生成できる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)

		// WHEN
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// THEN
		assert.NotNil(t, r)
	})
}

func TestNeedsRecovery(t *testing.T) {
	t.Run("Redo ログが空の場合はリカバリ不要", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// WHEN
		needs, err := r.NeedsRecovery()

		// THEN
		assert.NoError(t, err)
		assert.False(t, needs)
	})

	t.Run("COMMIT 済みの Redo ログがある場合はリカバリが必要", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)
		env.redoLog.AppendCommit(lock.TrxId(1))
		_ = env.redoLog.Flush()

		// WHEN
		needs, err := r.NeedsRecovery()

		// THEN
		assert.NoError(t, err)
		assert.True(t, needs)
	})

	t.Run("チェックポイント LSN 以前のレコードしかない場合はリカバリ不要", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)
		env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		_ = env.redoLog.Flush()
		_ = env.redoLog.SetCheckpointLsn(redo.Lsn(1))

		// WHEN
		needs, err := r.NeedsRecovery()

		// THEN
		assert.NoError(t, err)
		assert.False(t, needs)
	})
}

func TestExecute(t *testing.T) {
	t.Run("COMMIT 済みトランザクションはロールバックされない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)

		trxId := env.trxManager.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)
		_ = env.trxManager.Commit(trxId)

		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// WHEN
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		// レコードが残っている (ロールバックされていない)
		iter, err := table.primaryIndex.search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.next()
		assert.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("未 COMMIT のトランザクションがロールバックされる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)

		trxId := env.trxManager.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)

		// COMMIT せずに Redo ログにページ変更だけ記録してフラッシュ
		pgId := page.NewId(env.undoFileId, 0)
		readPage, _ := env.bp.PageForRead(pgId)
		env.redoLog.AppendPageCopy(trxId, pgId, *readPage.Page)
		_ = env.redoLog.Flush()

		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// WHEN
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		// レコードがロールバックされている
		iter, err := table.primaryIndex.search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("Redo ログが空の場合でもエラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// WHEN
		err := r.Execute()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("Execute 後に Redo ログがクリアされる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		env.redoLog.AppendCommit(lock.TrxId(1))
		_ = env.redoLog.Flush()
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// WHEN
		_ = r.Execute()

		// THEN
		needs, err := r.NeedsRecovery()
		assert.NoError(t, err)
		assert.False(t, needs)
	})

	t.Run("ROLLBACK 済みトランザクションは再ロールバックされない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// ROLLBACK レコードが Redo ログにある
		env.redoLog.AppendRollback(lock.TrxId(1))
		_ = env.redoLog.Flush()

		// WHEN
		err := r.Execute()

		// THEN
		assert.NoError(t, err)
	})
}

func TestApplyRedoLog(t *testing.T) {
	t.Run("Page LSN がレコードの LSN 以上ならスキップされる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// ページを取得して、Page LSN に大きな値を書き込む
		pgId := page.NewId(env.undoFileId, 0)
		writePage, err := env.bp.PageForWrite(pgId)
		assert.NoError(t, err)
		originalData := make([]byte, page.PageSize)
		copy(originalData, writePage.Page.ToBytes())

		// Redo レコードの LSN=1、Page LSN=10 → スキップされるはず
		writePage.Page.Header[0] = 0
		writePage.Page.Header[1] = 0
		writePage.Page.Header[2] = 0
		writePage.Page.Header[3] = 10 // Page LSN = 10

		// LSN=1 のページ変更レコードを Redo ログに記録
		newPageData := make([]byte, page.PageSize)
		newPageData[page.PageHeaderSize] = 0xFF // body の先頭を変える
		newPage, _ := page.NewPage(newPageData)
		env.redoLog.AppendPageCopy(lock.TrxId(1), pgId, *newPage)
		_ = env.redoLog.Flush()

		records, _ := env.redoLog.ReadAll()

		// WHEN
		err = r.applyRedoLog(records)

		// THEN
		assert.NoError(t, err)
		// ページが上書きされていないことを確認 (body の先頭は 0xFF ではない)
		readPage, _ := env.bp.PageForRead(pgId)
		assert.NotEqual(t, byte(0xFF), readPage.Page.Body[0])
	})
}

func TestApplyRollback(t *testing.T) {
	t.Run("COMMIT 済みと未 COMMIT が混在する場合は未 COMMIT のみロールバック", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		table := setupTableForRecoveryTest(t, env)

		trx1 := env.trxManager.Begin()
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trx1,
		)
		_ = env.trxManager.Commit(trx1)

		trx2 := env.trxManager.Begin()
		_ = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
			trx2,
		)

		// trx1 は COMMIT 済み (Commit 内で Redo ログに記録される)、trx2 は未 COMMIT
		pgId := page.NewId(env.undoFileId, 0)
		readPage, _ := env.bp.PageForRead(pgId)
		env.redoLog.AppendPageCopy(trx2, pgId, *readPage.Page)
		_ = env.redoLog.Flush()

		records, _ := env.redoLog.ReadAll()
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// WHEN
		err := r.applyRollback(records)

		// THEN
		assert.NoError(t, err)
		// trx1 のレコード ("Alice") は残り、trx2 のレコード ("Bob") はロールバックされる
		iter, _ := table.primaryIndex.search(SearchModeStart{})
		record, ok, _ := iter.next()
		assert.True(t, ok)
		assert.Equal(t, "1", record.Values[0])

		_, ok, _ = iter.next()
		assert.False(t, ok)
	})
}

// recoveryTestEnv はリカバリテスト用の環境
type recoveryTestEnv struct {
	bp         *buffer.Pool
	redoLog    *redo.Buffer
	trxManager *TrxManager
	undoFileId page.FileId
}

// setupRecoveryTestEnv はリカバリテスト用の環境を構築する
func setupRecoveryTestEnv(t *testing.T) *recoveryTestEnv {
	t.Helper()

	// Redo ログ用ディレクトリ
	_ = os.MkdirAll(config.BaseDir, 0o750)
	t.Cleanup(func() { _ = os.RemoveAll(config.BaseDir) })

	redoLog, err := redo.NewBuffer()
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Clear() })

	env := setupTableTestEnv(t)
	trxManager := NewTrxManager(env.ct, env.undoLog, redoLog, env.lock, env.bp)

	return &recoveryTestEnv{
		bp:         env.bp,
		redoLog:    redoLog,
		trxManager: trxManager,
		undoFileId: page.FileId(3),
	}
}

// setupTableForRecoveryTest はリカバリテスト用に Table を構築する
func setupTableForRecoveryTest(t *testing.T, env *recoveryTestEnv) *Table {
	t.Helper()
	table, err := NewTable(env.bp, env.trxManager.catalog, env.trxManager.undoLog, env.trxManager.lock, "users")
	if err != nil {
		t.Fatalf("Table の作成に失敗: %v", err)
	}
	return table
}

package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
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

func TestRecoveryNeedsRecovery(t *testing.T) {
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
		_, _ = env.redoLog.AppendCommit(lock.TrxId(1))
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
		_, _ = env.redoLog.AppendCommit(lock.TrxId(1)) // LSN=1
		_ = env.redoLog.Flush()
		_ = env.redoLog.SetCheckpointLsn(redo.Lsn(1))

		// WHEN
		needs, err := r.NeedsRecovery()

		// THEN
		assert.NoError(t, err)
		assert.False(t, needs)
	})
}

func TestRecoveryExecute(t *testing.T) {
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
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
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

		// COMMIT せずに Redo ログをフラッシュ (Insert 由来の mtr 内ページ変更は既に記録されている)
		_ = env.redoLog.Flush()

		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// WHEN
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		// レコードがロールバックされている
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
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
		_, _ = env.redoLog.AppendCommit(lock.TrxId(1))
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
		_, _ = env.redoLog.AppendRollback(lock.TrxId(1))
		_ = env.redoLog.Flush()

		// WHEN
		err := r.Execute()

		// THEN
		assert.NoError(t, err)
	})
}

func TestRecoveryApplyRedoLog(t *testing.T) {
	t.Run("Page LSN がレコードの LSN 以上ならスキップされる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// ページを取得して、Page LSN に大きな値を書き込む
		pgId := page.NewId(env.undoFileId, 0)
		writePage, err := env.bp.PageForWrite(pgId)
		assert.NoError(t, err)
		originalData := make([]byte, page.Size)
		copy(originalData, writePage.Data().Bytes())

		// Redo レコードの LSN=1、Page LSN=10 → スキップされるはず
		writePage.Data().Header()[0] = 0
		writePage.Data().Header()[1] = 0
		writePage.Data().Header()[2] = 0
		writePage.Data().Header()[3] = 10 // Page LSN = 10

		// LSN=1 のページ変更レコードを mtr 境界で囲んで Redo ログに記録
		newPageData := make([]byte, page.Size)
		newPageData[page.HeaderSize] = 0xFF // body の先頭を変える
		newPage, _ := page.NewPage(newPageData)
		_, _ = env.redoLog.AppendMtrStart(lock.TrxId(1))
		_, _ = env.redoLog.AppendPageCopy(lock.TrxId(1), pgId, newPage)
		_, _ = env.redoLog.AppendMtrEnd(lock.TrxId(1))
		_ = env.redoLog.Flush()

		records, _ := env.redoLog.ReadFrom(redo.Lsn(0))

		// WHEN
		err = r.applyRedoLog(records)

		// THEN
		assert.NoError(t, err)
		// ページが上書きされていないことを確認 (body の先頭は 0xFF ではない)
		readPage, _ := env.bp.PageForRead(pgId)
		assert.NotEqual(t, byte(0xFF), readPage.Data().Body()[0])
	})

	t.Run("完全な mtr のページ変更は適用される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)
		pgId := page.NewId(env.undoFileId, 0)

		// MtrStart, PageWrite, MtrEnd を順に記録
		_, _ = env.redoLog.AppendMtrStart(lock.TrxId(1))
		newPageData := make([]byte, page.Size)
		newPageData[page.HeaderSize] = 0xAA
		newPage, _ := page.NewPage(newPageData)
		_, _ = env.redoLog.AppendPageCopy(lock.TrxId(1), pgId, newPage)
		_, _ = env.redoLog.AppendMtrEnd(lock.TrxId(1))
		_ = env.redoLog.Flush()
		records, _ := env.redoLog.ReadFrom(redo.Lsn(0))

		// WHEN
		err := r.applyRedoLog(records)

		// THEN
		assert.NoError(t, err)
		readPage, _ := env.bp.PageForRead(pgId)
		assert.Equal(t, byte(0xAA), readPage.Data().Body()[0])
	})

	t.Run("MtrEnd を欠く mtr のページ変更は破棄される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)
		pgId := page.NewId(env.undoFileId, 0)

		// MtrStart, PageWrite だけ (MtrEnd なし)
		_, _ = env.redoLog.AppendMtrStart(lock.TrxId(1))
		newPageData := make([]byte, page.Size)
		newPageData[page.HeaderSize] = 0xBB
		newPage, _ := page.NewPage(newPageData)
		_, _ = env.redoLog.AppendPageCopy(lock.TrxId(1), pgId, newPage)
		_ = env.redoLog.Flush()
		records, _ := env.redoLog.ReadFrom(redo.Lsn(0))

		// WHEN
		err := r.applyRedoLog(records)

		// THEN
		assert.NoError(t, err)
		readPage, _ := env.bp.PageForRead(pgId)
		assert.NotEqual(t, byte(0xBB), readPage.Data().Body()[0])
	})

	t.Run("mtr 境界に囲まれていないページ変更は無視される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)
		pgId := page.NewId(env.undoFileId, 0)

		newPageData := make([]byte, page.Size)
		newPageData[page.HeaderSize] = 0xCC
		newPage, _ := page.NewPage(newPageData)
		_, _ = env.redoLog.AppendPageCopy(lock.TrxId(1), pgId, newPage)
		_ = env.redoLog.Flush()
		records, _ := env.redoLog.ReadFrom(redo.Lsn(0))

		// WHEN
		err := r.applyRedoLog(records)

		// THEN
		assert.NoError(t, err)
		readPage, _ := env.bp.PageForRead(pgId)
		assert.NotEqual(t, byte(0xCC), readPage.Data().Body()[0])
	})

	t.Run("複数の mtr が交互に並んでも完全な mtr だけが適用される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)
		completePgId := page.NewId(env.undoFileId, 0)
		incompletePgId := page.NewId(env.undoFileId, 1)
		_, _ = env.bp.AddPage(incompletePgId)

		// 別 trxId の mtr が交互: T1 完全 / T2 不完全 (MtrEnd なし)
		_, _ = env.redoLog.AppendMtrStart(lock.TrxId(1))
		_, _ = env.redoLog.AppendMtrStart(lock.TrxId(2))
		completeData := make([]byte, page.Size)
		completeData[page.HeaderSize] = 0xAA
		completePg, _ := page.NewPage(completeData)
		_, _ = env.redoLog.AppendPageCopy(lock.TrxId(1), completePgId, completePg)
		incompleteData := make([]byte, page.Size)
		incompleteData[page.HeaderSize] = 0xBB
		incompletePg, _ := page.NewPage(incompleteData)
		_, _ = env.redoLog.AppendPageCopy(lock.TrxId(2), incompletePgId, incompletePg)
		_, _ = env.redoLog.AppendMtrEnd(lock.TrxId(1))
		_ = env.redoLog.Flush()
		records, _ := env.redoLog.ReadFrom(redo.Lsn(0))

		// WHEN
		err := r.applyRedoLog(records)

		// THEN
		assert.NoError(t, err)
		completePage, _ := env.bp.PageForRead(completePgId)
		assert.Equal(t, byte(0xAA), completePage.Data().Body()[0])
		incompletePage, _ := env.bp.PageForRead(incompletePgId)
		assert.NotEqual(t, byte(0xBB), incompletePage.Data().Body()[0])
	})
}

func TestRecoveryApplyRollback(t *testing.T) {
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
		_, _ = env.redoLog.AppendPageCopy(trx2, pgId, readPage.Data())
		_ = env.redoLog.Flush()

		records, _ := env.redoLog.ReadFrom(redo.Lsn(0))
		r := NewRecovery(env.redoLog, env.bp, env.trxManager, env.undoFileId)

		// WHEN
		err := r.applyRollback(records)

		// THEN
		assert.NoError(t, err)
		// trx1 のレコード ("Alice") は残り、trx2 のレコード ("Bob") はロールバックされる
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, _ := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		record, ok, _ := iter.Next()
		assert.True(t, ok)
		assert.Equal(t, "1", record.values[0])

		_, ok, _ = iter.Next()
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

	env := setupTableTestEnv(t)
	trxManager := NewTrxManager(env.ct, env.undoLog, env.redoLog, env.lock, env.bp)

	return &recoveryTestEnv{
		bp:         env.bp,
		redoLog:    env.redoLog,
		trxManager: trxManager,
		undoFileId: page.FileId(3),
	}
}

// setupTableForRecoveryTest はリカバリテスト用に Table を構築する
func setupTableForRecoveryTest(t *testing.T, env *recoveryTestEnv) *Table {
	t.Helper()
	table, err := NewTable(env.bp, env.trxManager.catalog, env.trxManager.undoLog, env.trxManager.lock, env.redoLog, "users")
	if err != nil {
		t.Fatalf("Table の作成に失敗: %v", err)
	}
	return table
}

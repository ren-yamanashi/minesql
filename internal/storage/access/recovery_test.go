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

		trx := env.trxManager.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		_ = env.trxManager.Commit(trx)

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

		trx := env.trxManager.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)

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
		writePage, err := env.bp.Page(pgId)
		assert.NoError(t, err)
		originalData := make([]byte, page.Size)
		copy(originalData, writePage.Data().Bytes())

		// Redo レコードの LSN=1、Page LSN=10 → スキップされるはず
		writePage.WriteHeaderAt(0, []byte{0, 0, 0, 10})

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
		readPage, _ := env.bp.Page(pgId)
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
		readPage, _ := env.bp.Page(pgId)
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
		readPage, _ := env.bp.Page(pgId)
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
		readPage, _ := env.bp.Page(pgId)
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
		completePage, _ := env.bp.Page(completePgId)
		assert.Equal(t, byte(0xAA), completePage.Data().Body()[0])
		incompletePage, _ := env.bp.Page(incompletePgId)
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
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		_ = env.trxManager.Commit(trx1)

		trx2 := env.trxManager.Begin()
		_ = table.Insert(
			trx2,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		)

		pgId := page.NewId(env.undoFileId, 0)
		readPage, _ := env.bp.Page(pgId)
		_, _ = env.redoLog.AppendPageCopy(trx2.trxId, pgId, readPage.Data())
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

	// DDL 経由で生じたダーティーページと Redo レコードをクリーンな状態にする
	// (Recovery / Checkpoint テストは「初期状態 = ダーティーページなし・Redo 空」を前提とする)
	if err := env.bp.FlushAllPages(); err != nil {
		t.Fatalf("FlushAllPages に失敗: %v", err)
	}
	if err := env.redoLog.Clear(); err != nil {
		t.Fatalf("redoLog.Clear に失敗: %v", err)
	}

	// applyRedoLog テストは undo page (FileId 3, PageNumber 0) の Page LSN がリセット済みであることを前提とするため、
	// DDL でスタンプされた Page LSN を 0 に戻す
	resetMtr := buffer.NewMtr(env.bp)
	defer resetMtr.UnpinAll()
	pg, err := resetMtr.PageForWrite(page.NewId(page.FileId(3), 0))
	if err != nil {
		t.Fatalf("undo page の取得に失敗: %v", err)
	}
	pg.WriteHeaderAt(0, []byte{0, 0, 0, 0})

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

func TestRecoveryExecuteRestoresCommittedInsertFromRedo(t *testing.T) {
	t.Run("ディスク未反映の Commit 済み Insert がリカバリで復元される", func(t *testing.T) {
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
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId())
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		record := searchFirstPrimaryRecord(t, table2)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.values)
	})
}

func TestRecoveryExecuteSkipsAlreadyAppliedPagesByPageLsn(t *testing.T) {
	t.Run("ディスクに反映済みのページは Page LSN により上書きされない", func(t *testing.T) {
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
		assert.NoError(t, env.redoLog.Flush())
		assert.NoError(t, env.bp.FlushAllPages())

		dataFileId := table.primaryIndex.fileId()
		dataPageId, beforeBytes, beforeLsn := snapshotLatestPageWrite(t, env, dataFileId)
		assert.NotEqual(t, redo.Lsn(0), beforeLsn)

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId())
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		afterBytes, afterLsn := readPageBytes(t, env2.bp, dataPageId)
		assert.Equal(t, beforeLsn, afterLsn)
		assert.Equal(t, beforeBytes, afterBytes)
	})
}

func TestRecoveryExecuteDiscardsIncompleteMtr(t *testing.T) {
	t.Run("MtrEnd 無しの不完全 mtr は Recovery で適用されない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)
		flushBaseline(t, env)

		catalogPageId := page.NewId(page.FileId(0), page.PageNumber(0))
		beforeBytes, beforeLsn := readPageBytes(t, env.bp, catalogPageId)

		broken := makeBrokenPage(t)
		trxId := lock.TrxId(99)
		_, err := env.redoLog.AppendMtrStart(trxId)
		assert.NoError(t, err)
		_, err = env.redoLog.AppendPageCopy(trxId, catalogPageId, broken)
		assert.NoError(t, err)
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId())
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		afterBytes, afterLsn := readPageBytes(t, env2.bp, catalogPageId)
		assert.Equal(t, beforeLsn, afterLsn)
		assert.Equal(t, beforeBytes, afterBytes)
	})
}

func TestRecoveryExecuteRollbacksMultipleInsertsInOneTransaction(t *testing.T) {
	t.Run("未 Commit の複数 Insert はクラッシュ後すべてロールバックされる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)
		flushBaseline(t, env)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
		assert.NoError(t, err)

		trx := env.trxMgr.Begin()
		for _, row := range [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
			{"3", "Carol", "carol@example.com"},
		} {
			err = table.Insert(trx, []string{"id", "name", "email"}, row)
			assert.NoError(t, err)
		}
		assert.NoError(t, env.redoLog.Flush())
		assert.NoError(t, env.bp.FlushAllPages())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId())
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		mtr := buffer.NewMtr(env2.bp)
		defer mtr.UnpinAll()
		iter, err := table2.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestRecoveryExecuteRollbacksUncommittedUpdateAfterCommittedInsert(t *testing.T) {
	t.Run("Commit 済み Insert 後の未 Commit Update がクラッシュ後にロールバックされる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)
		flushBaseline(t, env)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
		assert.NoError(t, err)

		trx1 := env.trxMgr.Begin()
		err = table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trx1)
		assert.NoError(t, err)

		trx2 := env.trxMgr.Begin()
		record := currentReadFirst(t, table, trx2)
		err = table.Update(trx2, record, []string{"name"}, []string{"Bob"})
		assert.NoError(t, err)
		assert.NoError(t, env.redoLog.Flush())
		assert.NoError(t, env.bp.FlushAllPages())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId())
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		restored := searchFirstPrimaryRecord(t, table2)
		assert.Equal(t, "Alice", restored.values[1])
	})
}

// snapshotLatestPageWrite は指定 FileId の最後の PageWrite レコードの PageId・現在のバイト列・Page LSN を返す
func snapshotLatestPageWrite(t *testing.T, env *integrationEnv, fileId page.FileId) (page.Id, []byte, redo.Lsn) {
	t.Helper()
	records, err := env.redoLog.ReadFrom(redo.Lsn(0))
	assert.NoError(t, err)
	var target page.Id
	for _, rec := range records {
		if rec.Type() != redo.RecordTypePageWrite {
			continue
		}
		if rec.PageId().FileId() != fileId {
			continue
		}
		target = rec.PageId()
	}
	assert.NotEqual(t, page.Id{}, target)
	data, lsn := readPageBytes(t, env.bp, target)
	return target, data, lsn
}

// readPageBytes は指定 PageId のページ全体のバイト列と Page LSN を取得する
func readPageBytes(t *testing.T, bp *buffer.Pool, pageId page.Id) ([]byte, redo.Lsn) {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	bufPage, err := mtr.PageForRead(pageId)
	assert.NoError(t, err)
	bytes := make([]byte, page.Size)
	copy(bytes, bufPage.Data().Bytes())
	lsn := redo.Lsn(binary.BigEndian.Uint32(bufPage.Data().Header()))
	return bytes, lsn
}

// makeBrokenPage はテストで「不完全 mtr に詰める異常な内容のページ」を生成する
func makeBrokenPage(t *testing.T) *page.Page {
	t.Helper()
	data := make([]byte, page.Size)
	for i := range data {
		data[i] = 0xFF
	}
	pg, err := page.NewPage(data)
	assert.NoError(t, err)
	return pg
}

// flushBaseline はテーブル作成完了直後のベース状態を、Redo ログと全データページを揃ってディスクへ
// フラッシュすることで保存する。クラッシュシミュレーションのテストでベース構造を再オープン可能にする
func flushBaseline(t *testing.T, env *integrationEnv) {
	t.Helper()
	assert.NoError(t, env.redoLog.Flush())
	assert.NoError(t, env.bp.FlushAllPages())
}

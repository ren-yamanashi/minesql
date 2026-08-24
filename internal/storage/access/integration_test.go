package access

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestIntegrationCreateTable(t *testing.T) {
	t.Run("CreateTable で作成したテーブルを NewTable で開ける", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)

		// WHEN
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, table)
		assert.NotNil(t, table.primaryIndex)
		assert.Len(t, table.secondaryIndexes, 1)
	})
}

func TestIntegrationCommit(t *testing.T) {
	t.Run("Insert -> Commit -> Select でデータが読み取れる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx := env.trxMgr.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)

		// WHEN
		err = env.trxMgr.Commit(trx)

		// THEN
		assert.NoError(t, err)
		record := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.values)
	})

	t.Run("Insert -> Update -> Commit -> Select で更新後のデータが読み取れる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx := env.trxMgr.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)

		before := currentReadFirst(t, table, trx)
		err = table.Update(trx, before, []string{"name"}, []string{"Bob"})
		assert.NoError(t, err)

		// WHEN
		err = env.trxMgr.Commit(trx)

		// THEN
		assert.NoError(t, err)
		updated := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "1", updated.values[0])
		assert.Equal(t, "Bob", updated.values[1])
		assert.Equal(t, "alice@example.com", updated.values[2])
	})

	t.Run("Insert -> Delete -> Commit -> Select でデータが読み取れない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx := env.trxMgr.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)

		record := currentReadFirst(t, table, trx)
		err = table.SoftDelete(trx, record)
		assert.NoError(t, err)

		// WHEN
		err = env.trxMgr.Commit(trx)

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestIntegrationInsertLogsDataPageRedo(t *testing.T) {
	t.Run("実 Insert でデータページが Redo 記録され Page LSN がスタンプされる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		dataFileId := table.primaryIndex.fileId()

		// WHEN
		trx := env.trxMgr.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)

		// THEN: 主キーインデックス (データページ) の PageWrite レコードが記録されている
		assert.NoError(t, env.redoLog.Flush())
		records, err := env.redoLog.ReadFrom(redo.Lsn(0))
		assert.NoError(t, err)
		undoFileId := env.ct.UndoLogFileId()
		var dataWrite, firstUndo, firstData *redo.Record
		for i := range records {
			r := records[i]
			if r.Type() != redo.RecordTypePageWrite {
				continue
			}
			switch r.PageId().FileId() {
			case dataFileId:
				dataWrite = &records[i]
				if firstData == nil {
					firstData = &records[i]
				}
			case undoFileId:
				if firstUndo == nil {
					firstUndo = &records[i]
				}
			}
		}
		assert.NotNil(t, dataWrite, "データページの PageWrite レコードが記録されていない")

		// THEN: Undo ページの Redo はデータページの Redo より前に記録される (WAL 耐久性順序)
		assert.NotNil(t, firstUndo, "Undo ページの PageWrite レコードが記録されていない")
		assert.NotNil(t, firstData)
		assert.Less(t, firstUndo.Lsn(), firstData.Lsn())

		// THEN: 該当データページの Page LSN がスタンプされ、レコードの LSN と一致する
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		bufPage, err := mtr.PageForRead(dataWrite.PageId())
		assert.NoError(t, err)
		pageLsn := redo.Lsn(binary.BigEndian.Uint32(bufPage.Data().Header()))
		assert.NotEqual(t, redo.Lsn(0), pageLsn)
		assert.Equal(t, dataWrite.Lsn(), pageLsn)
	})
}

func TestIntegrationRollback(t *testing.T) {
	t.Run("Insert -> Rollback -> Select でデータが読み取れない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx := env.trxMgr.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)

		// WHEN
		err = env.trxMgr.Rollback(trx)

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("Insert -> Commit -> Update -> Rollback -> Select で元のデータに戻る", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx1 := env.trxMgr.Begin()
		err := table.Insert(
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

		// WHEN
		err = env.trxMgr.Rollback(trx2)

		// THEN
		assert.NoError(t, err)
		restored := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Alice", restored.values[1])
	})
}

func TestIntegrationMultipleTransactions(t *testing.T) {
	t.Run("Trx1 で Insert/Commit した後に Trx2 で読み取れる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx1 := env.trxMgr.Begin()
		err := table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trx1)
		assert.NoError(t, err)

		// WHEN
		trx2 := env.trxMgr.Begin()
		err = table.Insert(
			trx2,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trx2)
		assert.NoError(t, err)

		// THEN
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)

		r1, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", r1.values[1])

		r2, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Bob", r2.values[1])
	})
}

func TestIntegrationCrashRecovery(t *testing.T) {
	t.Run("Commit 済みの Insert がリカバリ後も残る", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx := env.trxMgr.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trx)
		assert.NoError(t, err)

		// WHEN
		r := NewRecovery(env.redoLog, env.bp, env.trxMgr, env.ct.UndoLogFileId(), env.ddlMgr)
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		record := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.values)
	})

	t.Run("未 Commit の Insert がリカバリでロールバックされる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx := env.trxMgr.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)

		err = env.redoLog.Flush()
		assert.NoError(t, err)

		// WHEN
		r := NewRecovery(env.redoLog, env.bp, env.trxMgr, env.ct.UndoLogFileId(), env.ddlMgr)
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("Commit 済みの Update がリカバリ後も反映されている", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trx1 := env.trxMgr.Begin()
		err := table.Insert(
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
		err = env.trxMgr.Commit(trx2)
		assert.NoError(t, err)

		// WHEN
		r := NewRecovery(env.redoLog, env.bp, env.trxMgr, env.ct.UndoLogFileId(), env.ddlMgr)
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		updated := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", updated.values[1])
	})
}

func TestIntegrationCrashRecoveryAfterPurge(t *testing.T) {
	t.Run("Purge 実行後にクラッシュしても物理削除された古バージョンは復活しない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		flushBaseline(t, env)

		trx1 := env.trxMgr.Begin()
		err := table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		record := currentReadFirst(t, table, trx2)
		assert.NoError(t, table.SoftDelete(trx2, record))
		assert.NoError(t, env.trxMgr.Commit(trx2))

		p := NewPurge(env.trxMgr)
		assert.NoError(t, p.purge())
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		mtr := buffer.NewMtr(env2.bp)
		defer mtr.UnpinAll()
		iter, err := table2.primaryIndex.tree.Search(mtr, btree.SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Get()
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestIntegrationCrashRecoveryAfterRollback(t *testing.T) {
	t.Run("通常 Rollback 後にクラッシュしてもロールバック結果が保たれる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		flushBaseline(t, env)

		trx1 := env.trxMgr.Begin()
		err := table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		record := currentReadFirst(t, table, trx2)
		assert.NoError(t, table.Update(trx2, record, []string{"name"}, []string{"Bob"}))
		assert.NoError(t, env.trxMgr.Rollback(trx2))
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		restored := searchFirstPrimaryRecord(t, table2)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, restored.values)
	})
}

func TestIntegrationCrashRecoveryAfterStatementRollback(t *testing.T) {
	t.Run("Unique セカンダリの dup key で文レベル rollback した後クラッシュしてもプライマリ・セカンダリが整合する", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTableWithUniqueEmail(t, env)
		flushBaseline(t, env)

		trx := env.trxMgr.Begin()
		assert.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "a@example.com"},
		))
		savepoint := trx.Savepoint()
		dupErr := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "a@example.com"},
		)
		assert.ErrorIs(t, dupErr, btree.ErrDuplicateKey)
		assert.NoError(t, env.trxMgr.RollbackToSavepoint(trx, savepoint))
		assert.NoError(t, env.trxMgr.Commit(trx))
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		assert.NoError(t, r.Execute())

		// THEN
		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		primaryKeys := collectPrimaryKeys(t, table2)
		assert.Equal(t, []string{"1"}, primaryKeys)

		idxName := findSecondaryIndex(t, table2, "idx_name")
		assert.Equal(t, [][2]string{{"Alice", "1"}}, collectSecondaryEntries(t, env2, idxName))
		idxEmail := findSecondaryIndex(t, table2, "idx_email")
		assert.Equal(t, [][2]string{{"a@example.com", "1"}}, collectSecondaryEntries(t, env2, idxEmail))
	})
}

// integrationEnv は統合テスト用の環境
type integrationEnv struct {
	bp      *buffer.Pool
	ct      *dictionary.Catalog
	undoLog *undo.Manager
	lockMgr *lock.Manager
	redoLog *redo.Buffer
	trxMgr  *TrxManager
	ddlMgr  *undo.DDLManager
}

// setupIntegrationEnv は CreateTable + TrxManager を使った統合テスト用環境を構築する
func setupIntegrationEnv(t *testing.T) *integrationEnv {
	t.Helper()

	_ = os.MkdirAll(config.BaseDir, 0o750)
	t.Cleanup(func() { _ = os.RemoveAll(config.BaseDir) })

	// カタログ用 HeapFile (FileId=0)
	catalogPath := filepath.Join(config.BaseDir, "catalog.db")
	catalogHf, err := file.NewHeapFile(catalogPath)
	if err != nil {
		t.Fatalf("カタログ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = catalogHf.Close() })

	redoLog, err := redo.NewBuffer(config.BaseDir)
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}

	bp := buffer.NewPool(page.Size*50, redoLog, nil)
	bp.RegisterHeapFile(page.FileId(0), catalogHf)

	catalogMtr := newBootstrapMtr(bp, redoLog)
	ct := dictionary.CreateCatalog(catalogMtr)
	if err := catalogMtr.Commit(); err != nil {
		t.Fatalf("Catalog Commit に失敗: %v", err)
	}

	// Undo 用 HeapFile (catalog が採番した FileId を使用)
	undoFileId := ct.UndoLogFileId()
	undoPath := filepath.Join(config.BaseDir, "undo.db")
	undoHf, err := file.NewHeapFile(undoPath)
	if err != nil {
		t.Fatalf("Undo HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = undoHf.Close() })
	bp.RegisterHeapFile(undoFileId, undoHf)

	undoMtr := newBootstrapMtr(bp, redoLog)
	undoMgr := undo.NewManager(undoMtr, undoFileId)
	if err := undoMtr.Commit(); err != nil {
		t.Fatalf("undo.Manager Commit に失敗: %v", err)
	}

	ddlMgr, err := undo.NewDDLManager(bp, dictionary.CatalogFileId, ct.DDLUndoRootPageId())
	if err != nil {
		t.Fatalf("undo.DDLManager の作成に失敗: %v", err)
	}

	lockMgr := lock.NewManager()
	trxMgr := NewTrxManager(ct, undoMgr, redoLog, lockMgr, bp, ddlMgr, 1)

	return &integrationEnv{
		bp:      bp,
		ct:      ct,
		undoLog: undoMgr,
		lockMgr: lockMgr,
		redoLog: redoLog,
		trxMgr:  trxMgr,
		ddlMgr:  ddlMgr,
	}
}

func TestIntegrationConcurrentStress(t *testing.T) {
	t.Run("並行 Insert がデータレースなく完了し件数が一致する", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)
		const workers = 4
		const opsPerWorker = 5

		// WHEN
		var wg sync.WaitGroup
		for w := range workers {
			wg.Add(1)
			go func(workerId int) {
				defer wg.Done()
				table, err := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
				if err != nil {
					return
				}
				for j := range opsPerWorker {
					trx := env.trxMgr.Begin()
					key := fmt.Sprintf("%04d", workerId*opsPerWorker+j+1)
					name := fmt.Sprintf("user_%d_%d", workerId, j)
					email := fmt.Sprintf("u%d-%d@example.com", workerId, j)
					_ = table.Insert(trx, []string{"id", "name", "email"}, []string{key, name, email})
					_ = env.trxMgr.Commit(trx)
				}
			}(w)
		}
		wg.Wait()

		// THEN: 全件挿入されている
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
		assert.NoError(t, err)
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		count := 0
		for {
			_, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			count++
		}
		assert.Equal(t, workers*opsPerWorker, count)
	})

	t.Run("並行 Insert / Update / Delete / Search がデータレースなく完了する", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)
		const writers = 3
		const insertPerWriter = 4 // 各 writer が 4 件挿入、1 件 update、1 件 delete

		// WHEN
		var wg sync.WaitGroup
		for w := range writers {
			wg.Add(1)
			go func(workerId int) {
				defer wg.Done()
				table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
				keyOf := func(j int) string {
					return fmt.Sprintf("%04d", workerId*insertPerWriter+j+1)
				}
				for j := range insertPerWriter {
					trx := env.trxMgr.Begin()
					_ = table.Insert(
						trx,
						[]string{"id", "name", "email"},
						[]string{keyOf(j), fmt.Sprintf("u%d-%d", workerId, j), fmt.Sprintf("u%d-%d@example.com", workerId, j)},
					)
					_ = env.trxMgr.Commit(trx)
				}
				trx := env.trxMgr.Begin()
				if rec := currentReadByPk(t, table, trx, keyOf(0)); rec != nil {
					_ = table.Update(trx, rec, []string{"name"}, []string{"updated"})
				}
				_ = env.trxMgr.Commit(trx)
				trx = env.trxMgr.Begin()
				if rec := currentReadByPk(t, table, trx, keyOf(insertPerWriter-1)); rec != nil {
					_ = table.SoftDelete(trx, rec)
				}
				_ = env.trxMgr.Commit(trx)
			}(w)
		}
		// Search reader (writer の操作中に並行して全件 scan を繰り返す)
		wg.Add(1)
		go func() {
			defer wg.Done()
			table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
			for range 10 {
				mtr := buffer.NewMtr(env.bp)
				iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
				if err != nil {
					mtr.UnpinAll()
					continue
				}
				for {
					_, ok, err := iter.Next()
					if err != nil || !ok {
						break
					}
				}
				mtr.UnpinAll()
			}
		}()
		wg.Wait()

		// THEN: 物理削除はされていないので最終的な scan で各 worker が SoftDelete した分はスキップされる
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		count := 0
		for {
			_, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			count++
		}
		assert.Equal(t, writers*(insertPerWriter-1), count)
	})

	t.Run("並行 Commit と並行 未 Commit が混在してもリカバリで整合性が保たれる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)
		const committedWorkers = 3
		const uncommittedWorkers = 3
		const opsPerWorker = 3

		// WHEN: Commit ワーカーと未 Commit ワーカーが並行に動く
		var wg sync.WaitGroup

		for w := range committedWorkers {
			wg.Add(1)
			go func(workerId int) {
				defer wg.Done()
				table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
				for j := range opsPerWorker {
					trx := env.trxMgr.Begin()
					key := fmt.Sprintf("%04d", workerId*opsPerWorker+j+1)
					_ = table.Insert(
						trx,
						[]string{"id", "name", "email"},
						[]string{key, fmt.Sprintf("u%d-%d", workerId, j), fmt.Sprintf("u%d-%d@example.com", workerId, j)},
					)
					_ = env.trxMgr.Commit(trx)
				}
			}(w)
		}

		for w := range uncommittedWorkers {
			wg.Add(1)
			go func(workerId int) {
				defer wg.Done()
				table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
				for j := range opsPerWorker {
					trx := env.trxMgr.Begin()
					key := fmt.Sprintf("9%03d", workerId*opsPerWorker+j+1)
					_ = table.Insert(
						trx,
						[]string{"id", "name", "email"},
						[]string{key, fmt.Sprintf("nc%d-%d", workerId, j), fmt.Sprintf("nc%d-%d@example.com", workerId, j)},
					)
					env.lockMgr.Release(trx.trxId)
				}
			}(w)
		}
		wg.Wait()

		_ = env.redoLog.Flush()

		// クラッシュリカバリ実行
		r := NewRecovery(env.redoLog, env.bp, env.trxMgr, env.ct.UndoLogFileId(), env.ddlMgr)
		err := r.Execute()
		assert.NoError(t, err)

		// THEN: Commit 済みの挿入のみ残り、未 Commit はロールバックされている
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, env.redoLog, "users")
		mtr := buffer.NewMtr(env.bp)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		count := 0
		for {
			_, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			count++
		}
		assert.Equal(t, committedWorkers*opsPerWorker, count)
	})
}

// createUsersTable は統合テスト用の users テーブルを作成する
func createUsersTable(t *testing.T, env *integrationEnv) *Table {
	t.Helper()
	table, err := CreateTable(env.trxMgr, CreateTableInput{
		TableName: "users",
		ColNames:  []string{"id", "name", "email"},
		PkCount:   1,
		Indexes: []CreateIndexInput{
			{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: dictionary.IndexTypeNonUnique},
		},
	})
	if err != nil {
		t.Fatalf("users テーブルの作成に失敗: %v", err)
	}
	return table
}

// createUsersTableWithUniqueEmail は idx_email を Unique として持つ統合テスト用の users テーブルを作成する
func createUsersTableWithUniqueEmail(t *testing.T, env *integrationEnv) *Table {
	t.Helper()
	table, err := CreateTable(env.trxMgr, CreateTableInput{
		TableName: "users",
		ColNames:  []string{"id", "name", "email"},
		PkCount:   1,
		Indexes: []CreateIndexInput{
			{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: dictionary.IndexTypeNonUnique},
			{IndexName: "idx_email", ColNames: []string{"email"}, IndexType: dictionary.IndexTypeUnique},
		},
	})
	if err != nil {
		t.Fatalf("users テーブルの作成に失敗: %v", err)
	}
	return table
}

// collectPrimaryKeys はテーブルの全プライマリレコードから id カラム (先頭カラム) の値を集める
func collectPrimaryKeys(t *testing.T, table *Table) []string {
	t.Helper()
	mtr := buffer.NewMtr(table.bufferPool)
	defer mtr.UnpinAll()
	iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
	if err != nil {
		t.Fatalf("primaryIndex.search に失敗: %v", err)
	}
	var keys []string
	for {
		rec, ok, err := iter.Next()
		if err != nil {
			t.Fatalf("iter.Next に失敗: %v", err)
		}
		if !ok {
			return keys
		}
		keys = append(keys, rec.values[0])
	}
}

// collectSecondaryEntries は指定セカンダリインデックスの全エントリを (sk, pk) ペアの列で返す
func collectSecondaryEntries(t *testing.T, env *integrationEnv, idx *secondaryIndex) [][2]string {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := idx.search(mtr, SearchModeStart{}, nil)
	if err != nil {
		t.Fatalf("secondaryIndex.search に失敗: %v", err)
	}
	var entries [][2]string
	for {
		rec, ok, err := iter.NextIndexOnly()
		if err != nil {
			t.Fatalf("iter.NextIndexOnly に失敗: %v", err)
		}
		if !ok {
			return entries
		}
		entries = append(entries, [2]string{rec.values[0], rec.pk[0]})
	}
}

// crashAndRecover はインメモリ状態 (バッファプール / トランザクションマネージャ / ロックマネージャ /
// REDO バッファ / UNDO マネージャ / カタログ) を破棄し、HeapFile と Redo ログファイルの
// ディスク内容のみを保持したまま新しい環境を構築する。クラッシュ → 再起動をシミュレートする。
//   - 旧 env のリソースは Close しない (テスト終了時の Cleanup でまとめて解放される)
//   - 呼び出し側は事前に prev.redoLog.Flush() を呼んでログをディスクに同期しておくこと
//   - tableNames は再オープン対象のテーブル名 (CreateTable で作成した <name>.db を再オープン)
func crashAndRecover(t *testing.T, prev *integrationEnv, tableNames []string) *integrationEnv {
	t.Helper()
	_ = prev // 旧 env はテスト終了時の Cleanup でまとめて解放されるため、ここでは参照しない

	redoLog, err := redo.NewBuffer(config.BaseDir)
	if err != nil {
		t.Fatalf("redo.Buffer の再オープンに失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Close() })

	bp := buffer.NewPool(page.Size*50, redoLog, nil)

	catalogPath := filepath.Join(config.BaseDir, "catalog.db")
	catalogHf, err := file.NewHeapFile(catalogPath)
	if err != nil {
		t.Fatalf("カタログ HeapFile の再オープンに失敗: %v", err)
	}
	t.Cleanup(func() { _ = catalogHf.Close() })
	bp.RegisterHeapFile(page.FileId(0), catalogHf)

	ct, err := dictionary.NewCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の再オープンに失敗: %v", err)
	}

	undoFileId := ct.UndoLogFileId()
	undoPath := filepath.Join(config.BaseDir, "undo.db")
	undoHf, err := file.NewHeapFile(undoPath)
	if err != nil {
		t.Fatalf("Undo HeapFile の再オープンに失敗: %v", err)
	}
	t.Cleanup(func() { _ = undoHf.Close() })
	bp.RegisterHeapFile(undoFileId, undoHf)

	for _, name := range tableNames {
		record, err := fetchTable(ct, bp, name)
		if err != nil {
			t.Fatalf("テーブル %q の取得に失敗: %v", name, err)
		}
		fileId := record.MetaPageId().FileId()
		tablePath := filepath.Join(config.BaseDir, fmt.Sprintf("%s.db", name))
		tableHf, err := file.NewHeapFile(tablePath)
		if err != nil {
			t.Fatalf("テーブル %q の HeapFile 再オープンに失敗: %v", name, err)
		}
		t.Cleanup(func() { _ = tableHf.Close() })
		bp.RegisterHeapFile(fileId, tableHf)
	}

	maxTrxId, err := redoLog.MaxUserTrxId()
	if err != nil {
		t.Fatalf("MaxUserTrxId の取得に失敗: %v", err)
	}
	initialNextTrxId := max(ct.NextTrxId(), maxTrxId+1)

	ddlMgr, err := undo.NewDDLManager(bp, dictionary.CatalogFileId, ct.DDLUndoRootPageId())
	if err != nil {
		t.Fatalf("undo.DDLManager の再オープンに失敗: %v", err)
	}

	lockMgr := lock.NewManager()
	// Recovery 中は undoMgr の entries を参照しないので、Recovery 実行用の仮の TrxManager を空 undoMgr 抜きで構成する
	tempTrxMgr := NewTrxManager(ct, nil, redoLog, lockMgr, bp, ddlMgr, initialNextTrxId)
	r := NewRecovery(redoLog, bp, tempTrxMgr, undoFileId, ddlMgr)
	if err := r.Execute(); err != nil {
		t.Fatalf("Recovery.Execute に失敗: %v", err)
	}

	undoMgr, err := undo.OpenManager(bp, redoLog, undoFileId)
	if err != nil {
		t.Fatalf("undo.Manager の再オープンに失敗: %v", err)
	}
	trxMgr := NewTrxManager(ct, undoMgr, redoLog, lockMgr, bp, ddlMgr, initialNextTrxId)

	return &integrationEnv{
		bp:      bp,
		ct:      ct,
		undoLog: undoMgr,
		lockMgr: lockMgr,
		redoLog: redoLog,
		trxMgr:  trxMgr,
		ddlMgr:  ddlMgr,
	}
}

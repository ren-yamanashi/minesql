package access

import (
	"os"
	"path/filepath"
	"testing"

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
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lockMgr, "users")

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

		trxId := env.trxMgr.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)

		// WHEN
		err = env.trxMgr.Commit(trxId)

		// THEN
		assert.NoError(t, err)
		record := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.values)
	})

	t.Run("Insert -> Update -> Commit -> Select で更新後のデータが読み取れる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trxId := env.trxMgr.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)

		before := searchFirstPrimaryRecord(t, table)
		err = table.Update(before, []string{"name"}, []string{"Bob"}, trxId)
		assert.NoError(t, err)

		// WHEN
		err = env.trxMgr.Commit(trxId)

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

		trxId := env.trxMgr.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)

		record := searchFirstPrimaryRecord(t, table)
		err = table.SoftDelete(record, trxId)
		assert.NoError(t, err)

		// WHEN
		err = env.trxMgr.Commit(trxId)

		// THEN
		assert.NoError(t, err)
		iter, err := table.primaryIndex.search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestIntegrationRollback(t *testing.T) {
	t.Run("Insert -> Rollback -> Select でデータが読み取れない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trxId := env.trxMgr.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)

		// WHEN
		err = env.trxMgr.Rollback(trxId)

		// THEN
		assert.NoError(t, err)
		iter, err := table.primaryIndex.search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("Insert -> Commit -> Update -> Rollback -> Select で元のデータに戻る", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trxId1 := env.trxMgr.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId1,
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trxId1)
		assert.NoError(t, err)

		trxId2 := env.trxMgr.Begin()
		record := searchFirstPrimaryRecord(t, table)
		err = table.Update(record, []string{"name"}, []string{"Bob"}, trxId2)
		assert.NoError(t, err)

		// WHEN
		err = env.trxMgr.Rollback(trxId2)

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

		trxId1 := env.trxMgr.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId1,
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trxId1)
		assert.NoError(t, err)

		// WHEN
		trxId2 := env.trxMgr.Begin()
		err = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
			trxId2,
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trxId2)
		assert.NoError(t, err)

		// THEN
		iter, err := table.primaryIndex.search(SearchModeStart{})
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

		trxId := env.trxMgr.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trxId)
		assert.NoError(t, err)

		// WHEN
		r := NewRecovery(env.redoLog, env.bp, env.trxMgr, env.ct.UndoLogFileId())
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

		trxId := env.trxMgr.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId,
		)
		assert.NoError(t, err)

		// Commit せずに Redo ログにページ変更を記録してフラッシュ (クラッシュを模擬)
		undoPageId := page.NewId(env.ct.UndoLogFileId(), 0)
		readPage, err := env.bp.PageForRead(undoPageId)
		assert.NoError(t, err)
		_, _ = env.redoLog.AppendPageCopy(trxId, undoPageId, readPage.Data())
		err = env.redoLog.Flush()
		assert.NoError(t, err)

		// WHEN
		r := NewRecovery(env.redoLog, env.bp, env.trxMgr, env.ct.UndoLogFileId())
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		iter, err := table.primaryIndex.search(SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("Commit 済みの Update がリカバリ後も反映されている", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)

		trxId1 := env.trxMgr.Begin()
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			trxId1,
		)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trxId1)
		assert.NoError(t, err)

		trxId2 := env.trxMgr.Begin()
		record := searchFirstPrimaryRecord(t, table)
		err = table.Update(record, []string{"name"}, []string{"Bob"}, trxId2)
		assert.NoError(t, err)
		err = env.trxMgr.Commit(trxId2)
		assert.NoError(t, err)

		// WHEN
		r := NewRecovery(env.redoLog, env.bp, env.trxMgr, env.ct.UndoLogFileId())
		err = r.Execute()

		// THEN
		assert.NoError(t, err)
		updated := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", updated.values[1])
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
}

// setupIntegrationEnv は CreateTable + TrxManager を使った統合テスト用環境を構築する
func setupIntegrationEnv(t *testing.T) *integrationEnv {
	t.Helper()

	_ = os.MkdirAll(config.BaseDir, 0o750)
	t.Cleanup(func() { _ = os.RemoveAll(config.BaseDir) })

	// カタログ用 HeapFile (FileId=0)
	catalogPath := filepath.Join(config.BaseDir, "catalog.db")
	catalogHf, err := file.NewHeapFile(page.FileId(0), catalogPath)
	if err != nil {
		t.Fatalf("カタログ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = catalogHf.Close() })

	bp := buffer.NewPool(page.Size*50, nil)
	bp.RegisterHeapFile(page.FileId(0), catalogHf)

	ct, err := dictionary.CreateCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の作成に失敗: %v", err)
	}

	// Undo 用 HeapFile (catalog が採番した FileId を使用)
	undoFileId := ct.UndoLogFileId()
	undoPath := filepath.Join(config.BaseDir, "undo.db")
	undoHf, err := file.NewHeapFile(undoFileId, undoPath)
	if err != nil {
		t.Fatalf("Undo HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = undoHf.Close() })
	bp.RegisterHeapFile(undoFileId, undoHf)

	redoLog, err := redo.NewBuffer(config.BaseDir)
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Clear() })

	undoMgr, err := undo.NewManager(bp, redoLog, undoFileId)
	if err != nil {
		t.Fatalf("undo.Manager の作成に失敗: %v", err)
	}

	lockMgr := lock.NewManager()
	trxMgr := NewTrxManager(ct, undoMgr, redoLog, lockMgr, bp)

	return &integrationEnv{
		bp:      bp,
		ct:      ct,
		undoLog: undoMgr,
		lockMgr: lockMgr,
		redoLog: redoLog,
		trxMgr:  trxMgr,
	}
}

// createUsersTable は統合テスト用の users テーブルを作成する
func createUsersTable(t *testing.T, env *integrationEnv) *Table {
	t.Helper()
	table, err := CreateTable(env.bp, env.undoLog, env.lockMgr, CreateTableInput{
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

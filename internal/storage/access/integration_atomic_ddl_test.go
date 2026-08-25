package access

import (
	"fmt"
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

func TestIntegrationAtomicDDLCrashAfterFileAllocation(t *testing.T) {
	t.Run("FileId 確保直後のクラッシュで物理ファイルが削除される", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		flushBaseline(t, env)
		fileId, tablePath := createTableUntilFileId(t, env, "users")
		fileIdBeforeCrash := fileId
		assert.NoError(t, env.bp.FlushAllPages())
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecoverWithPendingFiles(t, env, nil, []pendingTableFile{
			{fileId: fileId, path: tablePath},
		})

		// THEN
		_, statErr := os.Stat(tablePath)
		assert.True(t, os.IsNotExist(statErr))
		assertDDLUndoEmpty(t, env2)
		assertNextFileIdGreaterThan(t, env2, fileIdBeforeCrash)
	})
}

func TestIntegrationAtomicDDLCrashAfterPrimaryIndex(t *testing.T) {
	t.Run("プライマリ B+Tree 作成完了後のクラッシュで物理ファイルが削除される", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		flushBaseline(t, env)
		fileId, tablePath, _ := createTableUntilPrimary(t, env, "users", 1)
		fileIdBeforeCrash := fileId
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecoverWithPendingFiles(t, env, nil, []pendingTableFile{
			{fileId: fileId, path: tablePath},
		})

		// THEN
		_, statErr := os.Stat(tablePath)
		assert.True(t, os.IsNotExist(statErr))
		assertDDLUndoEmpty(t, env2)
		assertNextFileIdGreaterThan(t, env2, fileIdBeforeCrash)
	})
}

func TestIntegrationAtomicDDLCrashDuringMetaRegistration(t *testing.T) {
	t.Run("TableMeta 登録途中のクラッシュで登録済み Meta 行と物理ファイルが削除される", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		flushBaseline(t, env)
		colNames := []string{"id", "name", "email"}
		fileId, tablePath, _ := createTableUntilPartialMeta(t, env, "users", 1, colNames, 2)
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecoverWithPendingFiles(t, env, nil, []pendingTableFile{
			{fileId: fileId, path: tablePath},
		})

		// THEN
		_, statErr := os.Stat(tablePath)
		assert.True(t, os.IsNotExist(statErr))
		assertTableMetaAbsent(t, env2, "users")
		assertIndexMetaAbsentForFile(t, env2, fileId)
		assertColumnMetaAbsentForFile(t, env2, fileId)
		assertDDLUndoEmpty(t, env2)
	})
}

func TestIntegrationAtomicDDLCrashDuringSecondaryIndexBuild(t *testing.T) {
	t.Run("セカンダリインデックス作成途中のクラッシュで完成済みセカンダリも含めて全 Rollback される", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		flushBaseline(t, env)
		colNames := []string{"id", "name", "email"}
		indexes := []CreateIndexInput{
			{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: dictionary.IndexTypeNonUnique},
			{IndexName: "idx_email", ColNames: []string{"email"}, IndexType: dictionary.IndexTypeUnique},
		}
		fileId, tablePath, _, secondaries := createTableUntilSecondaryN(t, env, "users", 1, colNames, indexes, 1)
		assert.Len(t, secondaries, 1)
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecoverWithPendingFiles(t, env, nil, []pendingTableFile{
			{fileId: fileId, path: tablePath},
		})

		// THEN
		_, statErr := os.Stat(tablePath)
		assert.True(t, os.IsNotExist(statErr))
		assertTableMetaAbsent(t, env2, "users")
		assertIndexMetaAbsentForFile(t, env2, fileId)
		assertIndexKeyColumnMetaAbsentForIndex(t, env2, secondaries[0].indexId)
		assertColumnMetaAbsentForFile(t, env2, fileId)
		assertDDLUndoEmpty(t, env2)
	})
}

func TestIntegrationAtomicDDLCrashAfterCommit(t *testing.T) {
	t.Run("CreateTable 完了直後のクラッシュでテーブルは正常に open できる", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		_ = createUsersTable(t, env)
		flushBaseline(t, env)

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})

		// THEN
		table, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		assert.NotNil(t, table)
		assert.NotNil(t, table.primaryIndex)
		assert.Len(t, table.secondaryIndexes, 1)
		assertDDLUndoEmpty(t, env2)
	})
}

func TestIntegrationAtomicDDLDoubleCrashRecoveryIsIdempotent(t *testing.T) {
	t.Run("Recovery 完了後の再 Recovery が DDL Rollback を再実行しない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		flushBaseline(t, env)
		fileId, tablePath := createTableUntilFileId(t, env, "users")
		assert.NoError(t, env.redoLog.Flush())

		env2 := crashAndRecoverWithPendingFiles(t, env, nil, []pendingTableFile{
			{fileId: fileId, path: tablePath},
		})
		_, statErr := os.Stat(tablePath)
		assert.True(t, os.IsNotExist(statErr))
		assertDDLUndoEmpty(t, env2)

		// WHEN
		env3 := crashAndRecoverWithPendingFiles(t, env2, nil, nil)

		// THEN
		_, statErr2 := os.Stat(tablePath)
		assert.True(t, os.IsNotExist(statErr2))
		assertDDLUndoEmpty(t, env3)
		records, err := env3.redoLog.ReadFrom(redo.Lsn(0))
		assert.NoError(t, err)
		for _, rec := range records {
			assert.NotEqual(t, lock.DDLReservedTrxId, rec.TrxId())
		}
	})
}

func TestIntegrationAtomicDDLRuntimeRollbackDeletesFileAndRecoveryStillSucceeds(t *testing.T) {
	t.Run("実行時 DDL Rollback で物理ファイルが削除され、続くクラッシュ後の Recovery が成功する", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		flushBaseline(t, env)
		fileId, tablePath, _ := createTableUntilPrimary(t, env, "users", 1)
		assert.NoError(t, env.trxMgr.Rollback(env.trxMgr.BeginDDL()))
		_, statErr := os.Stat(tablePath)
		assert.True(t, os.IsNotExist(statErr))
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecoverWithPendingFiles(t, env, nil, nil)

		// THEN
		_, statErr2 := os.Stat(tablePath)
		assert.True(t, os.IsNotExist(statErr2))
		assertDDLUndoEmpty(t, env2)
		assertNextFileIdGreaterThan(t, env2, fileId)
	})
}

func TestIntegrationAtomicDDLCrashWithOrphanUndo(t *testing.T) {
	t.Run("undo 先行順序化の効果: メタ Insert 前に undo だけ書かれた状態でクラッシュしても孤児メタが残らない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		flushBaseline(t, env)
		colNames := []string{"id", "name", "email"}
		fileId, tablePath, _ := createTableUntilPartialMeta(t, env, "users", 1, colNames, len(colNames))
		orphanIndexId := allocateIndexIdInIsolatedMtr(t, env)
		orphanIndexRecord := dictionary.NewIndexMetaRecord(
			fileId,
			orphanIndexId,
			"idx_orphan",
			dictionary.IndexTypeNonUnique,
			1,
			page.NewId(fileId, 0),
		)
		appendMetaInsertUndoInIsolatedMtr(t, env, undo.MetaTableTypeIndex, orphanIndexRecord.Encode().Key())
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecoverWithPendingFiles(t, env, nil, []pendingTableFile{
			{fileId: fileId, path: tablePath},
		})

		// THEN
		_, statErr := os.Stat(tablePath)
		assert.True(t, os.IsNotExist(statErr))
		assertTableMetaAbsent(t, env2, "users")
		assertIndexMetaAbsentForFile(t, env2, fileId)
		assertColumnMetaAbsentForFile(t, env2, fileId)
		assertDDLUndoEmpty(t, env2)
	})
}

// appendMetaInsertUndoInIsolatedMtr は MetaInsertUndo を単独 mtr で Append・Commit する
//   - 対応する Meta Insert は行わないため、 「undo だけ書かれた不整合状態」 を作れる
func appendMetaInsertUndoInIsolatedMtr(
	t *testing.T,
	env *integrationEnv,
	metaTableType undo.MetaTableType,
	key []byte,
) {
	t.Helper()
	mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
	record := undo.NewDDLRecord(
		undo.DDLRecordTypeMetaInsert,
		undo.NewMetaInsertUndoRecord(metaTableType, key).Serialize(),
	)
	if err := env.trxMgr.ddlManager.Append(mtr, record); err != nil {
		t.Fatalf("DDLManager.Append に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
}

// pendingTableFile は CreateTable 途中で確保したテーブルファイルを表す
//   - 「TableMeta に登録されていないが DDL Undo に AllocateFileIdUndo が残っている」状態の FileId とパスを保持し、
//     再起動時の BufferPool に HeapFile を再 Register するために使う
type pendingTableFile struct {
	fileId page.FileId
	path   string
}

// createTableUntilFileId は CreateTable の FileId 採番ステップのみを実行する
//   - 戻り値: 確保した FileId と物理ファイルのパス
func createTableUntilFileId(t *testing.T, env *integrationEnv, name string) (page.FileId, string) {
	t.Helper()
	fileId, err := createTableFile(env.trxMgr.BeginDDL(), name)
	if err != nil {
		t.Fatalf("createTableFile に失敗: %v", err)
	}
	return fileId, filepath.Join(config.BaseDir, fmt.Sprintf("%s.db", name))
}

// createTableUntilPrimary は CreateTable の FileId 採番とプライマリ B+Tree 作成までを実行する
func createTableUntilPrimary(t *testing.T, env *integrationEnv, name string, pkCount int) (page.FileId, string, *primaryIndex) {
	t.Helper()
	fileId, tablePath := createTableUntilFileId(t, env, name)
	pi, err := createPrimaryIndex(env.trxMgr.BeginDDL(), fileId, pkCount)
	if err != nil {
		t.Fatalf("createPrimaryIndex に失敗: %v", err)
	}
	return fileId, tablePath, pi
}

// createTableUntilPartialMeta は createTableUntilPrimary に加えて TableMeta + IndexMeta + 部分的な ColumnMeta を登録する
//   - registerColCount: ColumnMeta を何件まで登録するか (= len(colNames) より小さい値で「途中まで登録された状態」を作る)
//   - 各登録は 1 mtr で行い、 直後に対応する MetaInsertUndo を DDL Undo 領域に Append する
func createTableUntilPartialMeta(
	t *testing.T,
	env *integrationEnv,
	name string,
	pkCount int,
	colNames []string,
	registerColCount int,
) (page.FileId, string, *primaryIndex) {
	t.Helper()
	fileId, tablePath, pi := createTableUntilPrimary(t, env, name, pkCount)

	tableRecord := dictionary.NewTableMetaRecord(name, pi.tree.MetaPageId(), len(colNames))
	insertMetaWithDDLUndo(t, env, undo.MetaTableTypeTable, tableRecord.Encode().Key(), func(mtr *buffer.Mtr) error {
		return env.ct.TableMeta().Insert(mtr, tableRecord)
	})

	indexId := allocateIndexIdInIsolatedMtr(t, env)
	indexRecord := dictionary.NewIndexMetaRecord(
		fileId,
		indexId,
		dictionary.PrimaryIndexName,
		dictionary.IndexTypePrimary,
		pkCount,
		pi.tree.MetaPageId(),
	)
	insertMetaWithDDLUndo(t, env, undo.MetaTableTypeIndex, indexRecord.Encode().Key(), func(mtr *buffer.Mtr) error {
		return env.ct.IndexMeta().Insert(mtr, indexRecord)
	})

	for i := range registerColCount {
		colRecord := dictionary.NewColumnMetaRecord(fileId, colNames[i], i)
		insertMetaWithDDLUndo(t, env, undo.MetaTableTypeColumn, colRecord.Encode().Key(), func(mtr *buffer.Mtr) error {
			return env.ct.ColumnMeta().Insert(mtr, colRecord)
		})
	}
	return fileId, tablePath, pi
}

// createTableUntilSecondaryN は createTableUntilPartialMeta (全件) に加えて、 セカンダリ N 件分を完成させる
//   - secondaryCount: 完成させるセカンダリインデックスの件数 (= len(indexes) より小さい値で「N+1 件目失敗」を模擬)
func createTableUntilSecondaryN(
	t *testing.T,
	env *integrationEnv,
	name string,
	pkCount int,
	colNames []string,
	indexes []CreateIndexInput,
	secondaryCount int,
) (page.FileId, string, *primaryIndex, []*secondaryIndex) {
	t.Helper()
	fileId, tablePath, pi := createTableUntilPartialMeta(t, env, name, pkCount, colNames, len(colNames))
	secondaries := make([]*secondaryIndex, 0, secondaryCount)
	for i := range secondaryCount {
		si, err := buildOneSecondaryIndex(env.trxMgr.BeginDDL(), fileId, pi.tree, indexes[i])
		if err != nil {
			t.Fatalf("buildOneSecondaryIndex に失敗: %v", err)
		}
		secondaries = append(secondaries, si)
	}
	return fileId, tablePath, pi, secondaries
}

// insertMetaWithDDLUndo は Meta テーブルへの 1 件の Insert と対応する MetaInsertUndo の Append を 1 mtr で実行・Commit する
func insertMetaWithDDLUndo(
	t *testing.T,
	env *integrationEnv,
	metaTableType undo.MetaTableType,
	key []byte,
	insertFn func(mtr *buffer.Mtr) error,
) {
	t.Helper()
	mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
	if err := insertFn(mtr); err != nil {
		mtr.UnpinAll()
		t.Fatalf("Meta Insert に失敗: %v", err)
	}
	record := undo.NewDDLRecord(
		undo.DDLRecordTypeMetaInsert,
		undo.NewMetaInsertUndoRecord(metaTableType, key).Serialize(),
	)
	if err := env.trxMgr.ddlManager.Append(mtr, record); err != nil {
		mtr.UnpinAll()
		t.Fatalf("DDLManager.Append に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
}

// allocateIndexIdInIsolatedMtr は AllocateIndexId を独立した mtr で呼び、 Commit して返す
func allocateIndexIdInIsolatedMtr(t *testing.T, env *integrationEnv) dictionary.IndexId {
	t.Helper()
	mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
	indexId, err := env.ct.AllocateIndexId(mtr)
	if err != nil {
		mtr.UnpinAll()
		t.Fatalf("AllocateIndexId に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
	return indexId
}

// crashAndRecoverWithPendingFiles は crashAndRecover と同様に新しい環境を構築するが、
// TableMeta に登録されていない pending DDL の FileId 用の HeapFile も Recovery 前に再 Register する
//   - registeredTables: TableMeta から fetchTable で解決できるテーブル名一覧
//   - pendingFiles: TableMeta に登録されていないが DDL Undo Rollback の対象になる物理ファイル
func crashAndRecoverWithPendingFiles(
	t *testing.T,
	prev *integrationEnv,
	registeredTables []string,
	pendingFiles []pendingTableFile,
) *integrationEnv {
	t.Helper()
	_ = prev

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

	for _, name := range registeredTables {
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

	for _, pending := range pendingFiles {
		if _, err := os.Stat(pending.path); os.IsNotExist(err) {
			continue
		}
		pendingHf, err := file.NewHeapFile(pending.path)
		if err != nil {
			t.Fatalf("pending HeapFile %q の再オープンに失敗: %v", pending.path, err)
		}
		t.Cleanup(func() { _ = pendingHf.Close() })
		bp.RegisterHeapFile(pending.fileId, pendingHf)
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
	tempTrxMgr := NewTrxManager(ct, nil, redoLog, lockMgr, bp, ddlMgr, initialNextTrxId)
	r := NewRecovery(redoLog, bp, tempTrxMgr, undoFileId, ddlMgr)
	if err := r.Execute(); err != nil {
		t.Fatalf("Recovery.Execute に失敗: %v", err)
	}

	// Recovery の Redo Replay でヘッダーページの nextFileId / nextIndexId が更新されるが、
	// Recovery 前に作った ct はそれらを in-memory に反映しないため、 再 open して最新値を読み直す
	refreshedCt, err := dictionary.NewCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の再 open に失敗: %v", err)
	}
	refreshedNextTrxId := max(refreshedCt.NextTrxId(), maxTrxId+1)

	refreshedDdlMgr, err := undo.NewDDLManager(bp, dictionary.CatalogFileId, refreshedCt.DDLUndoRootPageId())
	if err != nil {
		t.Fatalf("undo.DDLManager の再 open に失敗: %v", err)
	}

	undoMgr, err := undo.OpenManager(bp, redoLog, undoFileId)
	if err != nil {
		t.Fatalf("undo.Manager の再オープンに失敗: %v", err)
	}
	trxMgr := NewTrxManager(refreshedCt, undoMgr, redoLog, lockMgr, bp, refreshedDdlMgr, refreshedNextTrxId)

	return &integrationEnv{
		bp:      bp,
		ct:      refreshedCt,
		undoLog: undoMgr,
		lockMgr: lockMgr,
		redoLog: redoLog,
		trxMgr:  trxMgr,
		ddlMgr:  refreshedDdlMgr,
	}
}

func assertDDLUndoEmpty(t *testing.T, env *integrationEnv) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	records, err := env.trxMgr.ddlManager.ReverseScan(mtr)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func assertNextFileIdGreaterThan(t *testing.T, env *integrationEnv, fileIdBefore page.FileId) {
	t.Helper()
	mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
	nextFileId, err := env.ct.AllocateFileId(mtr)
	assert.NoError(t, err)
	assert.Greater(t, nextFileId, fileIdBefore)
	assert.NoError(t, mtr.Commit())
}

func assertTableMetaAbsent(t *testing.T, env *integrationEnv, name string) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.ct.TableMeta().Search(mtr, dictionary.SearchModeStart{})
	assert.NoError(t, err)
	for {
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		if !ok {
			return
		}
		assert.NotEqual(t, name, record.Name())
	}
}

func assertIndexMetaAbsentForFile(t *testing.T, env *integrationEnv, fileId page.FileId) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.ct.IndexMeta().Search(mtr, dictionary.SearchModeStart{})
	assert.NoError(t, err)
	for {
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		if !ok {
			return
		}
		assert.NotEqual(t, fileId, record.FileId())
	}
}

func assertColumnMetaAbsentForFile(t *testing.T, env *integrationEnv, fileId page.FileId) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.ct.ColumnMeta().Search(mtr, dictionary.SearchModeStart{})
	assert.NoError(t, err)
	for {
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		if !ok {
			return
		}
		assert.NotEqual(t, fileId, record.FileId())
	}
}

func assertIndexKeyColumnMetaAbsentForIndex(t *testing.T, env *integrationEnv, indexId dictionary.IndexId) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.ct.IndexKeyColumnMeta().Search(mtr, dictionary.SearchModeStart{})
	assert.NoError(t, err)
	for {
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		if !ok {
			return
		}
		assert.NotEqual(t, indexId, record.IndexId())
	}
}

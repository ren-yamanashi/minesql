package access

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestNewDDLRollbacker(t *testing.T) {
	t.Run("BufferPool と Catalog を保持する", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)

		// WHEN
		rollbacker := NewDDLRollbacker(env.bp, env.ct)

		// THEN
		assert.NotNil(t, rollbacker)
		assert.Same(t, env.bp, rollbacker.bufferPool)
		assert.Same(t, env.ct, rollbacker.catalog)
	})
}

func TestDDLRollbackerRollback(t *testing.T) {
	t.Run("DDLRecordTypeCreateBTree で対象 B+Tree の全ページを解放し次の割り当てで解放ページが再利用される", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		createMtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
		tree, err := btree.CreateTree(env.bp, page.FileId(2), createMtr)
		assert.NoError(t, err)
		assert.NoError(t, createMtr.Commit())
		usedBefore := collectUsedPageNumbers(t, env.bp, page.FileId(2))
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeCreateBTree,
			undo.NewCreateBTreeUndoRecord(tree.MetaPageId()).Serialize(),
		)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN: 使用中は予約ページのみ + 次の割り当てが解放済み範囲のページ番号になる
		assertFileUsedPagesAreReservedOnly(t, env.bp, page.FileId(2))
		reallocMtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
		reused, err := fsp.AllocatePage(reallocMtr, page.FileId(2))
		assert.NoError(t, err)
		assert.NoError(t, reallocMtr.Commit())
		assert.Contains(t, usedBefore, reused.PageNumber())
	})

	t.Run("DDLRecordTypeMetaInsert で TableMeta の Delete が呼ばれる", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewTableMetaRecord("users", page.NewId(page.FileId(2), page.PageNumber(0)), 3)
		insertTableMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeTable, insertedRecord.Encode().Key()).Serialize(),
		)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertTableMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeMetaInsert で IndexMeta の Delete が呼ばれる", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewIndexMetaRecord(
			page.FileId(2),
			dictionary.IndexId(1),
			dictionary.PrimaryIndexName,
			dictionary.IndexTypePrimary,
			1,
			page.NewId(page.FileId(2), page.PageNumber(0)),
		)
		insertIndexMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeIndex, insertedRecord.Encode().Key()).Serialize(),
		)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertIndexMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeMetaInsert で IndexKeyColumnMeta の Delete が呼ばれる", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewIndexKeyColumnMetaRecord(dictionary.IndexId(1), "id", 0)
		insertIndexKeyColumnMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeIndexKeyColumn, insertedRecord.Encode().Key()).Serialize(),
		)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertIndexKeyColumnMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeMetaInsert で ColumnMeta の Delete が呼ばれる", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewColumnMetaRecord(page.FileId(2), "name", 0)
		insertColumnMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeColumn, insertedRecord.Encode().Key()).Serialize(),
		)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertColumnMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeMetaInsert で ConstraintMeta の Delete が呼ばれる", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewConstraintMetaRecord(page.FileId(2), "id", "PRIMARY", page.FileId(0), "")
		insertConstraintMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeConstraint, insertedRecord.Encode().Key()).Serialize(),
		)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertConstraintMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeAllocateFileId で対象ファイルが物理削除される", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		targetFileId := page.FileId(99)
		targetPath := registerDDLRollbackerHeapFile(t, env.bp, targetFileId)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeAllocateFileId,
			undo.NewAllocateFileIdUndoRecord(targetFileId).Serialize(),
		)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		_, statErr := os.Stat(targetPath)
		assert.True(t, os.IsNotExist(statErr))
	})

	t.Run("未知の DDLRecordType を渡すとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		invalid := undo.NewDDLRecord(undo.DDLRecordType(99), nil)

		// WHEN
		mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		defer mtr.UnpinAll()
		_, err := rollbacker.Rollback(mtr, invalid)

		// THEN
		assert.Error(t, err)
	})
}

func TestDDLRollbackerRollbackIdempotent(t *testing.T) {
	t.Run("DDLRecordTypeCreateBTree の Rollback を 2 回連続実行してもエラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		createMtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
		tree, err := btree.CreateTree(env.bp, page.FileId(2), createMtr)
		assert.NoError(t, err)
		assert.NoError(t, createMtr.Commit())
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeCreateBTree,
			undo.NewCreateBTreeUndoRecord(tree.MetaPageId()).Serialize(),
		)
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)
		assertFileUsedPagesAreReservedOnly(t, env.bp, page.FileId(2))

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertFileUsedPagesAreReservedOnly(t, env.bp, page.FileId(2))
	})

	t.Run("DDLRecordTypeMetaInsert (TableMeta) の Rollback を 2 回連続実行してもエラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewTableMetaRecord("users", page.NewId(page.FileId(2), page.PageNumber(0)), 3)
		insertTableMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeTable, insertedRecord.Encode().Key()).Serialize(),
		)
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertTableMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeMetaInsert (IndexMeta) の Rollback を 2 回連続実行してもエラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewIndexMetaRecord(
			page.FileId(2),
			dictionary.IndexId(1),
			dictionary.PrimaryIndexName,
			dictionary.IndexTypePrimary,
			1,
			page.NewId(page.FileId(2), page.PageNumber(0)),
		)
		insertIndexMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeIndex, insertedRecord.Encode().Key()).Serialize(),
		)
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertIndexMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeMetaInsert (IndexKeyColumnMeta) の Rollback を 2 回連続実行してもエラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewIndexKeyColumnMetaRecord(dictionary.IndexId(1), "id", 0)
		insertIndexKeyColumnMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeIndexKeyColumn, insertedRecord.Encode().Key()).Serialize(),
		)
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertIndexKeyColumnMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeMetaInsert (ColumnMeta) の Rollback を 2 回連続実行してもエラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewColumnMetaRecord(page.FileId(2), "name", 0)
		insertColumnMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeColumn, insertedRecord.Encode().Key()).Serialize(),
		)
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertColumnMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeMetaInsert (ConstraintMeta) の Rollback を 2 回連続実行してもエラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		insertedRecord := dictionary.NewConstraintMetaRecord(page.FileId(2), "id", "PRIMARY", page.FileId(0), "")
		insertConstraintMeta(t, env, insertedRecord)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeConstraint, insertedRecord.Encode().Key()).Serialize(),
		)
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertConstraintMetaEmpty(t, env)
	})

	t.Run("DDLRecordTypeAllocateFileId の Rollback を 2 回連続実行してもエラーにならない", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		targetFileId := page.FileId(99)
		targetPath := registerDDLRollbackerHeapFile(t, env.bp, targetFileId)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeAllocateFileId,
			undo.NewAllocateFileIdUndoRecord(targetFileId).Serialize(),
		)
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		_, statErr := os.Stat(targetPath)
		assert.True(t, os.IsNotExist(statErr))
	})

	t.Run("DDLRecordTypeMetaInsert の Rollback は対象キーが最初から存在しなくても成功する", func(t *testing.T) {
		// GIVEN: Meta 挿入を行わずに、対応キーの Rollback だけを試みる (undo 先行順序で挿入前に失敗した状態を模擬)
		env := setupDDLRollbackerTestEnv(t)
		missingRecord := dictionary.NewTableMetaRecord("missing", page.NewId(page.FileId(2), page.PageNumber(0)), 1)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeMetaInsert,
			undo.NewMetaInsertUndoRecord(undo.MetaTableTypeTable, missingRecord.Encode().Key()).Serialize(),
		)

		// WHEN
		rollbackDDLUntilDone(t, env.bp, env.redoLog, rollbacker, record)

		// THEN
		assertTableMetaEmpty(t, env)
	})
}

// ddlRollbackerTestEnv は DDLRollbacker テスト用の最小環境
type ddlRollbackerTestEnv struct {
	bp      *buffer.Pool
	ct      *dictionary.Catalog
	redoLog *redo.Buffer
}

// setupDDLRollbackerTestEnv は DDLRollbacker のテストに必要な BufferPool + Catalog を構築する
func setupDDLRollbackerTestEnv(t *testing.T) *ddlRollbackerTestEnv {
	t.Helper()

	catalogPath := filepath.Join(t.TempDir(), "catalog.db")
	catalogHf, err := file.NewHeapFile(catalogPath)
	if err != nil {
		t.Fatalf("カタログ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = catalogHf.Close() })

	dataPath := filepath.Join(t.TempDir(), "data.db")
	dataHf, err := file.NewHeapFile(dataPath)
	if err != nil {
		t.Fatalf("データ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = dataHf.Close() })

	redoLog, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Close() })

	bp := buffer.NewPool(page.Size*50, redoLog, nil)
	bp.RegisterHeapFile(page.FileId(0), catalogHf)
	bp.RegisterHeapFile(page.FileId(2), dataHf)

	dataInitMtr := newBootstrapMtr(bp, redoLog)
	if err := fsp.InitHeader(dataInitMtr, page.FileId(2)); err != nil {
		dataInitMtr.UnpinAll()
		t.Fatalf("データファイルの FSP ヘッダー初期化に失敗: %v", err)
	}
	if err := dataInitMtr.Commit(); err != nil {
		t.Fatalf("データファイルの FSP ヘッダー Commit に失敗: %v", err)
	}

	catalogMtr := newBootstrapMtr(bp, redoLog)
	ct := dictionary.CreateCatalog(catalogMtr)
	if err := catalogMtr.Commit(); err != nil {
		t.Fatalf("Catalog Commit に失敗: %v", err)
	}

	return &ddlRollbackerTestEnv{bp: bp, ct: ct, redoLog: redoLog}
}

// rollbackDDLUntilDone は Rollback を done まで独立 mtr で繰り返し、CreateBTree の step 型に対応する
func rollbackDDLUntilDone(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer, rollbacker *DDLRollbacker, record undo.DDLRecord) {
	t.Helper()
	const maxSteps = 10000
	for step := range maxSteps {
		mtr := buffer.NewWriteMtr(bp, lock.DDLReservedTrxId, redoLog)
		done, err := rollbacker.Rollback(mtr, record)
		if err != nil {
			mtr.UnpinAll()
			t.Fatalf("Rollback に失敗: %v", err)
		}
		if err := mtr.Commit(); err != nil {
			t.Fatalf("Commit に失敗: %v", err)
		}
		if done {
			return
		}
		if step == maxSteps-1 {
			t.Fatalf("Rollback が %d step 経っても完了しない", maxSteps)
		}
	}
}

// assertFileUsedPagesAreReservedOnly はファイル全域を fsp.IsPageFree で走査し、
// 使用中ページが FSP ヘッダー (page 0) のみであることを検証する
//   - 対象ファイルは「1 tree だけを作成 → その tree を全解放した」状態であることを前提とする
//   - tree 解放後は inode ページ内の全スロットが空になり、inode ページ自体も単ページ解放される
func assertFileUsedPagesAreReservedOnly(t *testing.T, bp *buffer.Pool, fileId page.FileId) {
	t.Helper()
	used := collectUsedPageNumbers(t, bp, fileId)
	assert.Equal(t, []page.PageNumber{0}, used)
}

// collectUsedPageNumbers はファイル全域を fsp.IsPageFree で走査し、使用中ページの PageNumber を昇順で返す
//   - 走査範囲は先頭 4,096 ページ (テストで使うファイルサイズを十分に覆う。フリーリミット以上は fsp 側で常に free)
func collectUsedPageNumbers(t *testing.T, bp *buffer.Pool, fileId page.FileId) []page.PageNumber {
	t.Helper()
	const maxPageNumber = page.PageNumber(4096)
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	var used []page.PageNumber
	for pn := page.PageNumber(0); pn < maxPageNumber; pn++ {
		isFree, err := fsp.IsPageFree(mtr, page.NewId(fileId, pn))
		if err != nil {
			t.Fatalf("IsPageFree に失敗 (pageNumber=%d): %v", pn, err)
		}
		if !isFree {
			used = append(used, pn)
		}
	}
	return used
}

func insertTableMeta(t *testing.T, env *ddlRollbackerTestEnv, record dictionary.TableMetaRecord) {
	t.Helper()
	mtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
	defer mtr.UnpinAll()
	if err := env.ct.TableMeta().Insert(mtr, record); err != nil {
		t.Fatalf("TableMeta.Insert に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
}

func insertIndexMeta(t *testing.T, env *ddlRollbackerTestEnv, record dictionary.IndexMetaRecord) {
	t.Helper()
	mtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
	defer mtr.UnpinAll()
	if err := env.ct.IndexMeta().Insert(mtr, record); err != nil {
		t.Fatalf("IndexMeta.Insert に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
}

func insertIndexKeyColumnMeta(t *testing.T, env *ddlRollbackerTestEnv, record dictionary.IndexKeyColumnMetaRecord) {
	t.Helper()
	mtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
	defer mtr.UnpinAll()
	if err := env.ct.IndexKeyColumnMeta().Insert(mtr, record); err != nil {
		t.Fatalf("IndexKeyColumnMeta.Insert に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
}

func insertColumnMeta(t *testing.T, env *ddlRollbackerTestEnv, record dictionary.ColumnMetaRecord) {
	t.Helper()
	mtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
	defer mtr.UnpinAll()
	if err := env.ct.ColumnMeta().Insert(mtr, record); err != nil {
		t.Fatalf("ColumnMeta.Insert に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
}

func insertConstraintMeta(t *testing.T, env *ddlRollbackerTestEnv, record dictionary.ConstraintMetaRecord) {
	t.Helper()
	mtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
	defer mtr.UnpinAll()
	if err := env.ct.ConstraintMeta().Insert(mtr, record); err != nil {
		t.Fatalf("ConstraintMeta.Insert に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
}

func assertTableMetaEmpty(t *testing.T, env *ddlRollbackerTestEnv) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.ct.TableMeta().Search(mtr, dictionary.SearchModeStart{})
	assert.NoError(t, err)
	_, ok, err := iter.Next()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func assertIndexMetaEmpty(t *testing.T, env *ddlRollbackerTestEnv) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.ct.IndexMeta().Search(mtr, dictionary.SearchModeStart{})
	assert.NoError(t, err)
	_, ok, err := iter.Next()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func assertIndexKeyColumnMetaEmpty(t *testing.T, env *ddlRollbackerTestEnv) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.ct.IndexKeyColumnMeta().Search(mtr, dictionary.SearchModeStart{})
	assert.NoError(t, err)
	_, ok, err := iter.Next()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func assertColumnMetaEmpty(t *testing.T, env *ddlRollbackerTestEnv) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.ct.ColumnMeta().Search(mtr, dictionary.SearchModeStart{})
	assert.NoError(t, err)
	_, ok, err := iter.Next()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func assertConstraintMetaEmpty(t *testing.T, env *ddlRollbackerTestEnv) {
	t.Helper()
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.ct.ConstraintMeta().Search(mtr, dictionary.SearchModeStart{})
	assert.NoError(t, err)
	_, ok, err := iter.Next()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func registerDDLRollbackerHeapFile(t *testing.T, bp *buffer.Pool, fileId page.FileId) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "extra.db")
	hf, err := file.NewHeapFile(path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	bp.RegisterHeapFile(fileId, hf)
	return path
}

// assertPageFree は指定 PageId が fsp 的に free であることを検証する
func assertPageFree(t *testing.T, bp *buffer.Pool, id page.Id) {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	isFree, err := fsp.IsPageFree(mtr, id)
	if err != nil {
		t.Fatalf("IsPageFree に失敗: %v", err)
	}
	if !isFree {
		t.Fatalf("page %v が解放されていない", id)
	}
}

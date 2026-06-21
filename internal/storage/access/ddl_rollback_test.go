package access

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
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
	t.Run("DDLRecordTypeCreateBTree で対象 B+Tree の全ページを Deallocate する", func(t *testing.T) {
		// GIVEN
		env := setupDDLRollbackerTestEnv(t)
		createMtr := buffer.NewWriteMtr(env.bp, lock.SystemReservedTrxId, env.redoLog)
		tree, err := btree.CreateTree(env.bp, page.FileId(2), createMtr)
		assert.NoError(t, err)
		assert.NoError(t, createMtr.Commit())
		pageIds := collectTreePageIds(t, env.bp, tree)
		rollbacker := NewDDLRollbacker(env.bp, env.ct)
		record := undo.NewDDLRecord(
			undo.DDLRecordTypeCreateBTree,
			undo.NewCreateBTreeUndoRecord(tree.MetaPageId()).Serialize(),
		)

		// WHEN
		mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err = rollbacker.Rollback(mtr, record)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

		// THEN: 子→親の順で解放されるため最後に積まれた pageIds[0] (= メタページ) が head に来る
		head := readDDLFreeListHead(t, env.bp, env.ct.FreeListMapPageId(), page.FileId(2))
		assert.Equal(t, pageIds[0].PageNumber(), head)
		assert.GreaterOrEqual(t, len(pageIds), 2)
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
		mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(mtr, record)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

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
		mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(mtr, record)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

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
		mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(mtr, record)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

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
		mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(mtr, record)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

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
		mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(mtr, record)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

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
		mtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(mtr, record)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

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
		err := rollbacker.Rollback(mtr, invalid)

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
		firstMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		assert.NoError(t, rollbacker.Rollback(firstMtr, record))
		assert.NoError(t, firstMtr.Commit())
		firstHead := readDDLFreeListHead(t, env.bp, env.ct.FreeListMapPageId(), page.FileId(2))

		// WHEN
		secondMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err = rollbacker.Rollback(secondMtr, record)
		assert.NoError(t, secondMtr.Commit())

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, firstHead, readDDLFreeListHead(t, env.bp, env.ct.FreeListMapPageId(), page.FileId(2)))
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
		firstMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		assert.NoError(t, rollbacker.Rollback(firstMtr, record))
		assert.NoError(t, firstMtr.Commit())

		// WHEN
		secondMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(secondMtr, record)
		assert.NoError(t, secondMtr.Commit())

		// THEN
		assert.NoError(t, err)
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
		firstMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		assert.NoError(t, rollbacker.Rollback(firstMtr, record))
		assert.NoError(t, firstMtr.Commit())

		// WHEN
		secondMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(secondMtr, record)
		assert.NoError(t, secondMtr.Commit())

		// THEN
		assert.NoError(t, err)
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
		firstMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		assert.NoError(t, rollbacker.Rollback(firstMtr, record))
		assert.NoError(t, firstMtr.Commit())

		// WHEN
		secondMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(secondMtr, record)
		assert.NoError(t, secondMtr.Commit())

		// THEN
		assert.NoError(t, err)
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
		firstMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		assert.NoError(t, rollbacker.Rollback(firstMtr, record))
		assert.NoError(t, firstMtr.Commit())

		// WHEN
		secondMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(secondMtr, record)
		assert.NoError(t, secondMtr.Commit())

		// THEN
		assert.NoError(t, err)
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
		firstMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		assert.NoError(t, rollbacker.Rollback(firstMtr, record))
		assert.NoError(t, firstMtr.Commit())

		// WHEN
		secondMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(secondMtr, record)
		assert.NoError(t, secondMtr.Commit())

		// THEN
		assert.NoError(t, err)
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
		firstMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		assert.NoError(t, rollbacker.Rollback(firstMtr, record))
		assert.NoError(t, firstMtr.Commit())

		// WHEN
		secondMtr := buffer.NewWriteMtr(env.bp, lock.DDLReservedTrxId, env.redoLog)
		err := rollbacker.Rollback(secondMtr, record)
		assert.NoError(t, secondMtr.Commit())

		// THEN
		assert.NoError(t, err)
		_, statErr := os.Stat(targetPath)
		assert.True(t, os.IsNotExist(statErr))
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
	catalogHf, err := file.NewHeapFile(page.FileId(0), catalogPath)
	if err != nil {
		t.Fatalf("カタログ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = catalogHf.Close() })

	dataPath := filepath.Join(t.TempDir(), "data.db")
	dataHf, err := file.NewHeapFile(page.FileId(2), dataPath)
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

	ct, err := dictionary.CreateCatalog(bp, redoLog)
	if err != nil {
		t.Fatalf("Catalog の作成に失敗: %v", err)
	}

	return &ddlRollbackerTestEnv{bp: bp, ct: ct, redoLog: redoLog}
}

// collectTreePageIds は対象 B+Tree の全ページ ID を AllPageIds で取得する
func collectTreePageIds(t *testing.T, bp *buffer.Pool, tree *btree.Tree) []page.Id {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	pageIds, err := tree.AllPageIds(mtr)
	if err != nil {
		t.Fatalf("AllPageIds に失敗: %v", err)
	}
	return pageIds
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
	hf, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	bp.RegisterHeapFile(fileId, hf)
	return path
}

func readDDLFreeListHead(t *testing.T, bp *buffer.Pool, freeListMapPageId page.Id, fileId page.FileId) page.PageNumber {
	t.Helper()
	bufPage, err := bp.Page(freeListMapPageId)
	if err != nil {
		t.Fatalf("freeListMap ページの取得に失敗: %v", err)
	}
	defer bp.Unpin(freeListMapPageId)
	body := bufPage.Data().Body()
	count := binary.BigEndian.Uint32(body[0:4])
	for i := range int(count) {
		offset := 4 + i*8
		entryFileId := page.FileId(binary.BigEndian.Uint32(body[offset : offset+4]))
		if entryFileId != fileId {
			continue
		}
		return page.PageNumber(binary.BigEndian.Uint32(body[offset+4 : offset+8]))
	}
	return page.MaxPageNumber
}

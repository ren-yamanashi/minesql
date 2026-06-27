package access

import (
	"os"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestTrxManagerBeginDDL(t *testing.T) {
	t.Run("DDLReservedTrxId を持つ Transaction を返す", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		ddlTrx := tm.BeginDDL()

		// THEN
		assert.Equal(t, lock.DDLReservedTrxId, ddlTrx.trxId)
		assert.Equal(t, trxStateActive, ddlTrx.state)
	})

	t.Run("払い出された Transaction が TrxManager と同一の resource 参照を持つ", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)

		// WHEN
		ddlTrx := tm.BeginDDL()

		// THEN
		assert.Same(t, tm.bufferPool, ddlTrx.bufferPool)
		assert.Same(t, tm.redoLog, ddlTrx.redoLog)
		assert.Same(t, tm.lock, ddlTrx.lockMgr)
		assert.Same(t, tm.undoLog, ddlTrx.undoLog)
		assert.Same(t, tm.catalog, ddlTrx.catalog)
	})
}

func TestTrxManagerCommitDDL(t *testing.T) {
	t.Run("Commit で DDL Undo 領域の中身がクリアされる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		ddlTrx := tm.BeginDDL()
		appendSampleDDLUndo(t, tm)
		assertDDLUndoCount(t, tm, 1)

		// WHEN
		err := tm.Commit(ddlTrx)

		// THEN
		assert.NoError(t, err)
		assertDDLUndoCount(t, tm, 0)
	})

	t.Run("Commit 後も DDLUndoRootPageId は変わらない (= 永続コンテナ型)", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		ddlTrx := tm.BeginDDL()
		appendSampleDDLUndo(t, tm)
		before := tm.catalog.DDLUndoRootPageId()

		// WHEN
		err := tm.Commit(ddlTrx)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, before, tm.catalog.DDLUndoRootPageId())
	})

	t.Run("Commit 後にトランザクションが Inactive になる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		ddlTrx := tm.BeginDDL()

		// WHEN
		err := tm.Commit(ddlTrx)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, trxStateInactive, ddlTrx.state)
	})
}

func TestTrxManagerRollbackDDL(t *testing.T) {
	t.Run("Rollback で DDL Undo 領域の中身がクリアされる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		ddlTrx := tm.BeginDDL()
		appendMetaInsertSample(t, tm)
		assertDDLUndoCount(t, tm, 1)

		// WHEN
		err := tm.Rollback(ddlTrx)

		// THEN
		assert.NoError(t, err)
		assertDDLUndoCount(t, tm, 0)
	})

	t.Run("Rollback 後も DDLUndoRootPageId は変わらない (= 永続コンテナ型)", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		ddlTrx := tm.BeginDDL()
		appendMetaInsertSample(t, tm)
		before := tm.catalog.DDLUndoRootPageId()

		// WHEN
		err := tm.Rollback(ddlTrx)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, before, tm.catalog.DDLUndoRootPageId())
	})

	t.Run("Rollback で CreateBTreeUndo の B+Tree ページが解放される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		ddlTrx := tm.BeginDDL()
		treeFileId := page.FileId(2) // setupTrxManager で登録済み
		tree := appendCreateBTreeSample(t, tm, treeFileId)
		pageIds := collectTreePageIds(t, tm.bufferPool, tree)
		assert.GreaterOrEqual(t, len(pageIds), 2)

		// WHEN
		err := tm.Rollback(ddlTrx)
		assert.NoError(t, err)

		// THEN: 子→親の順で解放されるため、 最後に積まれた pageIds[0] (= メタページ) が freeListMap の head に来る
		head := readDDLFreeListHead(t, tm.bufferPool, tm.catalog.FreeListMapPageId(), treeFileId)
		assert.Equal(t, pageIds[0].PageNumber(), head)
	})

	t.Run("Rollback で MetaInsertUndo の Meta レコードが物理削除される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		ddlTrx := tm.BeginDDL()
		fileId := appendMetaInsertSample(t, tm)

		// WHEN
		err := tm.Rollback(ddlTrx)
		assert.NoError(t, err)

		// THEN: column meta が物理削除されている
		assertColumnMetaAbsent(t, tm, fileId, "test_col")
	})

	t.Run("Rollback で AllocateFileIdUndo の物理ファイルが削除される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		ddlTrx := tm.BeginDDL()
		targetFileId := page.FileId(99)
		targetPath := registerDDLRollbackerHeapFile(t, tm.bufferPool, targetFileId)
		appendAllocateFileIdUndoForTest(t, tm, targetFileId)

		// WHEN
		err := tm.Rollback(ddlTrx)
		assert.NoError(t, err)

		// THEN: 物理ファイルが削除されている
		_, statErr := os.Stat(targetPath)
		assert.True(t, os.IsNotExist(statErr))
	})

	t.Run("Rollback 後にトランザクションが Inactive になる", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		ddlTrx := tm.BeginDDL()

		// WHEN
		err := tm.Rollback(ddlTrx)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, trxStateInactive, ddlTrx.state)
	})
}

// appendSampleDDLUndo はテスト用にダミーの DDL Undo レコードを 1 件 Append する
func appendSampleDDLUndo(t *testing.T, tm *TrxManager) {
	t.Helper()
	mtr := buffer.NewWriteMtr(tm.bufferPool, lock.DDLReservedTrxId, tm.redoLog)
	record := undo.NewDDLRecord(
		undo.DDLRecordTypeAllocateFileId,
		undo.NewAllocateFileIdUndoRecord(99).Serialize(),
	)
	if err := tm.ddlManager.Append(mtr, record); err != nil {
		t.Fatalf("DDL Undo Append に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("DDL Undo Append mtr.Commit に失敗: %v", err)
	}
}

// assertDDLUndoCount は DDL Undo 領域に積まれた件数を ReverseScan で確認する
func assertDDLUndoCount(t *testing.T, tm *TrxManager, expected int) {
	t.Helper()
	mtr := buffer.NewMtr(tm.bufferPool)
	defer mtr.UnpinAll()
	records, err := tm.ddlManager.ReverseScan(mtr)
	if err != nil {
		t.Fatalf("ReverseScan に失敗: %v", err)
	}
	assert.Equal(t, expected, len(records))
}

// newColumnMetaForTest はテスト用の ColumnMetaRecord を作る
func newColumnMetaForTest(fileId page.FileId, colName string) dictionary.ColumnMetaRecord {
	return dictionary.NewColumnMetaRecord(fileId, colName, 0)
}

// appendMetaInsertSample はテスト用に ColumnMeta 1 件を Insert し、 対応する MetaInsertUndo を Append する
//   - return: 採番された fileId (= ColumnMeta の所属テーブル)
func appendMetaInsertSample(t *testing.T, tm *TrxManager) page.FileId {
	t.Helper()
	mtr := buffer.NewWriteMtr(tm.bufferPool, lock.DDLReservedTrxId, tm.redoLog)
	fileId, err := tm.catalog.AllocateFileId(mtr)
	if err != nil {
		mtr.UnpinAll()
		t.Fatalf("AllocateFileId に失敗: %v", err)
	}
	colRecord := newColumnMetaForTest(fileId, "test_col")
	colKey := colRecord.Encode().Key()
	if err := tm.catalog.ColumnMeta().Insert(mtr, colRecord); err != nil {
		mtr.UnpinAll()
		t.Fatalf("ColumnMeta Insert に失敗: %v", err)
	}
	if err := appendMetaInsertUndo(tm.systemTrx, mtr, undo.MetaTableTypeColumn, colKey); err != nil {
		mtr.UnpinAll()
		t.Fatalf("MetaInsertUndo Append に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("mtr.Commit に失敗: %v", err)
	}
	return fileId
}

// assertColumnMetaAbsent は指定 fileId・colName の ColumnMetaRecord が存在しないことを検証する
func assertColumnMetaAbsent(t *testing.T, tm *TrxManager, fileId page.FileId, colName string) {
	t.Helper()
	mtr := buffer.NewMtr(tm.bufferPool)
	defer mtr.UnpinAll()
	iter, err := tm.catalog.ColumnMeta().Search(mtr, dictionary.SearchModeStart{})
	if err != nil {
		t.Fatalf("ColumnMeta Search に失敗: %v", err)
	}
	for {
		record, ok, err := iter.Next()
		if err != nil {
			t.Fatalf("ColumnMeta Iter Next に失敗: %v", err)
		}
		if !ok {
			break
		}
		if record.FileId() == fileId && record.Name() == colName {
			t.Errorf("ColumnMeta %d.%s が残存している", fileId, colName)
			return
		}
	}
}

// appendCreateBTreeSample はテスト用に B+Tree を 1 つ作成し、 対応する CreateBTreeUndo を Append する
func appendCreateBTreeSample(t *testing.T, tm *TrxManager, fileId page.FileId) *btree.Tree {
	t.Helper()
	mtr := buffer.NewWriteMtr(tm.bufferPool, lock.DDLReservedTrxId, tm.redoLog)
	tree, err := btree.CreateTree(tm.bufferPool, fileId, mtr)
	if err != nil {
		mtr.UnpinAll()
		t.Fatalf("CreateTree に失敗: %v", err)
	}
	record := undo.NewDDLRecord(
		undo.DDLRecordTypeCreateBTree,
		undo.NewCreateBTreeUndoRecord(tree.MetaPageId()).Serialize(),
	)
	if err := tm.ddlManager.Append(mtr, record); err != nil {
		mtr.UnpinAll()
		t.Fatalf("CreateBTreeUndo Append に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("mtr.Commit に失敗: %v", err)
	}
	return tree
}

// appendAllocateFileIdUndoForTest はテスト用に AllocateFileIdUndo を Append する
func appendAllocateFileIdUndoForTest(t *testing.T, tm *TrxManager, fileId page.FileId) {
	t.Helper()
	mtr := buffer.NewWriteMtr(tm.bufferPool, lock.DDLReservedTrxId, tm.redoLog)
	record := undo.NewDDLRecord(
		undo.DDLRecordTypeAllocateFileId,
		undo.NewAllocateFileIdUndoRecord(fileId).Serialize(),
	)
	if err := tm.ddlManager.Append(mtr, record); err != nil {
		mtr.UnpinAll()
		t.Fatalf("AllocateFileIdUndo Append に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("mtr.Commit に失敗: %v", err)
	}
}

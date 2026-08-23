package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestResolvePrevVersion(t *testing.T) {
	t.Run("rollPtr が NullPointer の場合 nil を返す", func(t *testing.T) {
		// GIVEN
		env := setupVersionTestEnv(t)
		record := newTestPrimaryRecord(t, env.iter, "1", "Alice", "a@example.com", lock.TrxId(10), undo.NullPointer())

		// WHEN
		prev, err := env.resolve(record)

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, prev)
	})

	t.Run("Undo が Insert ならチェーン終端で nil を返す", func(t *testing.T) {
		// GIVEN
		env := setupVersionTestEnv(t)
		current := newTestPrimaryRecord(t, env.iter, "1", "Alice", "a@example.com", lock.TrxId(10), undo.NullPointer())
		insertUndo := undo.NewInsertRecord(page.FileId(2), current.Encode())
		ptr := env.appendUndo(t, lock.TrxId(10), undo.RecordTypeInsert, insertUndo)
		current.setRollPtr(ptr)

		// WHEN
		prev, err := env.resolve(current)

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, prev)
	})

	t.Run("Undo が Update なら旧バージョンが PrevRecord から再構築される", func(t *testing.T) {
		// GIVEN
		env := setupVersionTestEnv(t)
		prevTrxId := lock.TrxId(5)
		prevPtr := undo.NewPointer(7, 64)
		oldRecord := newTestPrimaryRecord(t, env.iter, "1", "old", "old@example.com", prevTrxId, prevPtr)
		newRecord := newTestPrimaryRecord(t, env.iter, "1", "new", "new@example.com", lock.TrxId(10), undo.NullPointer())
		updateUndo := undo.NewUpdateRecord(page.FileId(2), oldRecord.Encode(), newRecord.Encode(), prevTrxId, prevPtr)
		ptr := env.appendUndo(t, lock.TrxId(10), undo.RecordTypeUpdate, updateUndo)
		newRecord.setRollPtr(ptr)

		// WHEN
		prev, err := env.resolve(newRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, prev)
		assert.Equal(t, []string{"1", "old", "old@example.com"}, prev.values)
		assert.Equal(t, prevTrxId, prev.lastTrxId)
		assert.Equal(t, prevPtr, prev.rollPtr)
		assert.Equal(t, byte(0), prev.deleteMark)
	})

	t.Run("Undo が Delete なら旧バージョンが Record から再構築される", func(t *testing.T) {
		// GIVEN
		env := setupVersionTestEnv(t)
		prevTrxId := lock.TrxId(3)
		prevPtr := undo.NewPointer(4, 32)
		oldRecord := newTestPrimaryRecord(t, env.iter, "1", "Alice", "a@example.com", prevTrxId, prevPtr)
		deletedRecord := newTestPrimaryRecord(t, env.iter, "1", "Alice", "a@example.com", lock.TrxId(10), undo.NullPointer())
		deletedRecord.deleteMark = 1
		deleteUndo := undo.NewDeleteRecord(page.FileId(2), oldRecord.Encode(), prevTrxId, prevPtr)
		ptr := env.appendUndo(t, lock.TrxId(10), undo.RecordTypeDelete, deleteUndo)
		deletedRecord.setRollPtr(ptr)

		// WHEN
		prev, err := env.resolve(deletedRecord)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, prev)
		assert.Equal(t, []string{"1", "Alice", "a@example.com"}, prev.values)
		assert.Equal(t, prevTrxId, prev.lastTrxId)
		assert.Equal(t, prevPtr, prev.rollPtr)
		assert.Equal(t, byte(0), prev.deleteMark)
	})

	t.Run("Update を 2 段重ねたチェーンを遡れる", func(t *testing.T) {
		// GIVEN
		env := setupVersionTestEnv(t)
		// v1: trx 1 が INSERT
		v1 := newTestPrimaryRecord(t, env.iter, "1", "v1", "a@example.com", lock.TrxId(1), undo.NullPointer())
		insertUndo := undo.NewInsertRecord(page.FileId(2), v1.Encode())
		ptrInsert := env.appendUndo(t, lock.TrxId(1), undo.RecordTypeInsert, insertUndo)
		v1.setRollPtr(ptrInsert)

		// v2: trx 2 が UPDATE
		v2 := newTestPrimaryRecord(t, env.iter, "1", "v2", "a@example.com", lock.TrxId(2), undo.NullPointer())
		updateUndo1 := undo.NewUpdateRecord(page.FileId(2), v1.Encode(), v2.Encode(), lock.TrxId(1), ptrInsert)
		ptrUpdate1 := env.appendUndo(t, lock.TrxId(2), undo.RecordTypeUpdate, updateUndo1)
		v2.setRollPtr(ptrUpdate1)

		// v3 (current): trx 3 が UPDATE
		v3 := newTestPrimaryRecord(t, env.iter, "1", "v3", "a@example.com", lock.TrxId(3), undo.NullPointer())
		updateUndo2 := undo.NewUpdateRecord(page.FileId(2), v2.Encode(), v3.Encode(), lock.TrxId(2), ptrUpdate1)
		ptrUpdate2 := env.appendUndo(t, lock.TrxId(3), undo.RecordTypeUpdate, updateUndo2)
		v3.setRollPtr(ptrUpdate2)

		// WHEN: 1 段遡る
		prev1, err := env.resolve(v3)
		assert.NoError(t, err)
		assert.NotNil(t, prev1)
		assert.Equal(t, []string{"1", "v2", "a@example.com"}, prev1.values)
		assert.Equal(t, lock.TrxId(2), prev1.lastTrxId)

		// WHEN: もう 1 段遡る
		prev2, err := env.resolve(prev1)
		assert.NoError(t, err)
		assert.NotNil(t, prev2)
		assert.Equal(t, []string{"1", "v1", "a@example.com"}, prev2.values)
		assert.Equal(t, lock.TrxId(1), prev2.lastTrxId)

		// WHEN: もう 1 段遡ると終端
		prev3, err := env.resolve(prev2)

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, prev3)
	})
}

// versionTestEnv は resolvePrevVersion テスト用の環境
type versionTestEnv struct {
	iter    *iteratorTestEnv
	undoLog *undo.Manager
}

func (e *versionTestEnv) mtr() *buffer.Mtr {
	return buffer.NewMtr(e.iter.bp)
}

func (e *versionTestEnv) resolve(record *PrimaryRecord) (*PrimaryRecord, error) {
	return resolvePrevVersion(resolvePrevVersionInput{
		mtr:        e.mtr(),
		undoLog:    e.undoLog,
		catalog:    e.iter.ct,
		bufferPool: e.iter.bp,
		fileId:     page.FileId(2),
		record:     record,
	})
}

// setupVersionTestEnv は version テスト用の環境を構築する
func setupVersionTestEnv(t *testing.T) *versionTestEnv {
	t.Helper()
	iter := setupIteratorTestEnv(t)

	undoPath := filepath.Join(t.TempDir(), "undo.db")
	undoHf, err := file.NewHeapFile(undoPath)
	if err != nil {
		t.Fatalf("Undo HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = undoHf.Close() })
	iter.bp.RegisterHeapFile(page.FileId(3), undoHf)

	undoBootstrapMtr := newBootstrapMtr(iter.bp, iter.redoLog)
	undoMgr := undo.NewManager(undoBootstrapMtr, page.FileId(3))
	if err := undoBootstrapMtr.Commit(); err != nil {
		t.Fatalf("undo.Manager Commit に失敗: %v", err)
	}
	return &versionTestEnv{iter: iter, undoLog: undoMgr}
}

// newTestPrimaryRecord は version テスト用に PrimaryRecord を構築する
func newTestPrimaryRecord(t *testing.T, env *iteratorTestEnv, id, name, email string, trxId lock.TrxId, rollPtr undo.Pointer) *PrimaryRecord {
	t.Helper()
	pr, err := NewPrimaryRecord(env.ct, env.bp, NewPrimaryRecordInput{
		fileId:     page.FileId(2),
		pkCount:    1,
		deleteMark: 0,
		lastTrxId:  trxId,
		rollPtr:    rollPtr,
		colNames:   []string{"id", "name", "email"},
		values:     []string{id, name, email},
	})
	if err != nil {
		t.Fatalf("PrimaryRecord の作成に失敗: %v", err)
	}
	return pr
}

// appendUndoForTest は Undo レコードを Append して Pointer を返す
func (e *versionTestEnv) appendUndo(t *testing.T, trxId lock.TrxId, recordType undo.RecordType, record undo.Record) undo.Pointer {
	t.Helper()
	ptr, err := e.undoLog.Append(trxId, recordType, record)
	if err != nil {
		t.Fatalf("undo Append に失敗: %v", err)
	}
	return ptr
}

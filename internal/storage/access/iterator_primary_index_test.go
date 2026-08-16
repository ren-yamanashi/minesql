package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestPrimaryIndexIteratorNext(t *testing.T) {
	t.Run("プライマリレコードを取得できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})

		iter, mtr := searchPrimaryIndex(t, env)
		defer mtr.UnpinAll()

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"id", "name", "email"}, result.colNames)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, result.values)
	})

	t.Run("複数レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"2", "Bob", "b@example.com"})

		iter, mtr := searchPrimaryIndex(t, env)
		defer mtr.UnpinAll()

		// WHEN
		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "1", r1.values[0])

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "2", r2.values[0])

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("論理削除されたレコードをスキップする", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 1, []string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"2", "Bob", "b@example.com"})

		iter, mtr := searchPrimaryIndex(t, env)
		defer mtr.UnpinAll()

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "2", result.values[0])
	})

	t.Run("全レコードが論理削除済みの場合データなしを返す", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 1, []string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})

		iter, mtr := searchPrimaryIndex(t, env)
		defer mtr.UnpinAll()

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("空のインデックスから取得するとデータなしを返す", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		iter, mtr := searchPrimaryIndex(t, env)
		defer mtr.UnpinAll()

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// searchPrimaryIndex はプライマリ B+Tree を先頭から検索してイテレータと mtr を返す
//   - 呼び出し側は defer mtr.UnpinAll() で解放する
func searchPrimaryIndex(t *testing.T, env *iteratorTestEnv) (*PrimaryIndexIterator, *buffer.Mtr) {
	t.Helper()
	mode := SearchModeStart{}
	mtr := buffer.NewMtr(env.bp)
	iter, err := env.primaryTree.Search(mtr, mode.Encode())
	if err != nil {
		mtr.UnpinAll()
		t.Fatalf("プライマリインデックスの検索に失敗: %v", err)
	}
	return NewPrimaryIndexIterator(iter, env.ct, env.bp, page.FileId(2), nil, nil), mtr
}

func TestPrimaryIndexIteratorNextWithReadView(t *testing.T) {
	t.Run("最新が ReadView から可視ならそのまま返す", func(t *testing.T) {
		// GIVEN
		env := setupMVCCTestEnv(t)
		insertPrimaryRecordWithMvcc(t, env, lock.TrxId(1), undo.NullPointer(), "1", "Alice", "a@example.com")
		rv := newReadView(lock.TrxId(2), nil, lock.TrxId(2))
		iter, mtr := searchPrimaryIndexWithReadView(t, env, rv)
		defer mtr.UnpinAll()

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
	})

	t.Run("最新が不可視で Undo の旧バージョンが可視なら旧値を返す", func(t *testing.T) {
		// GIVEN
		env := setupMVCCTestEnv(t)
		insertedRecord := insertPrimaryRecordWithMvcc(t, env, lock.TrxId(1), undo.NullPointer(), "1", "Alice", "a@example.com")
		updateUndo := undo.NewUpdateRecord(page.FileId(2), insertedRecord.Encode(), btree.Record{}, lock.TrxId(1), undo.NullPointer())
		ptr := env.appendUndo(t, lock.TrxId(3), undo.RecordTypeUpdate, updateUndo)
		updatePrimaryRecordWithMvcc(t, env, lock.TrxId(3), ptr, "1", "Bob", "a@example.com")

		rv := newReadView(lock.TrxId(2), []lock.TrxId{lock.TrxId(3)}, lock.TrxId(4))
		iter, mtr := searchPrimaryIndexWithReadView(t, env, rv)
		defer mtr.UnpinAll()

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
	})

	t.Run("最新が不可視で Undo が Insert ならチェーン終端で空", func(t *testing.T) {
		// GIVEN
		env := setupMVCCTestEnv(t)
		insertedRecord := insertPrimaryRecordWithMvcc(t, env, lock.TrxId(3), undo.NullPointer(), "1", "Alice", "a@example.com")
		insertUndo := undo.NewInsertRecord(page.FileId(2), insertedRecord.Encode())
		ptr := env.appendUndo(t, lock.TrxId(3), undo.RecordTypeInsert, insertUndo)
		updatePrimaryRecordWithMvcc(t, env, lock.TrxId(3), ptr, "1", "Alice", "a@example.com")

		rv := newReadView(lock.TrxId(2), []lock.TrxId{lock.TrxId(3)}, lock.TrxId(4))
		iter, mtr := searchPrimaryIndexWithReadView(t, env, rv)
		defer mtr.UnpinAll()

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("可視だが deleteMark=1 のレコードはスキップされる", func(t *testing.T) {
		// GIVEN
		env := setupMVCCTestEnv(t)
		oldRecord := insertPrimaryRecordWithMvcc(t, env, lock.TrxId(1), undo.NullPointer(), "1", "Alice", "a@example.com")
		deleteUndo := undo.NewDeleteRecord(page.FileId(2), oldRecord.Encode(), lock.TrxId(1), undo.NullPointer())
		ptr := env.appendUndo(t, lock.TrxId(1), undo.RecordTypeDelete, deleteUndo)
		deleted, err := NewPrimaryRecord(env.iter.ct, env.iter.bp, NewPrimaryRecordInput{
			fileId:     page.FileId(2),
			pkCount:    1,
			deleteMark: 1,
			lastTrxId:  lock.TrxId(1),
			rollPtr:    ptr,
			colNames:   []string{"id", "name", "email"},
			values:     []string{"1", "Alice", "a@example.com"},
		})
		assert.NoError(t, err)
		updateMtr := buffer.NewMtr(env.iter.bp)
		assert.NoError(t, env.iter.primaryTree.Update(updateMtr, deleted.Encode()))
		updateMtr.UnpinAll()

		rv := newReadView(lock.TrxId(2), nil, lock.TrxId(2))
		iter, mtr := searchPrimaryIndexWithReadView(t, env, rv)
		defer mtr.UnpinAll()

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("Update を 2 段重ねた行は Read View 作成前のバージョンが見える", func(t *testing.T) {
		// GIVEN
		env := setupMVCCTestEnv(t)
		// v1: trx=1 INSERT (Alice)
		v1 := insertPrimaryRecordWithMvcc(t, env, lock.TrxId(1), undo.NullPointer(), "1", "Alice", "a@example.com")
		insertUndo := undo.NewInsertRecord(page.FileId(2), v1.Encode())
		ptr1 := env.appendUndo(t, lock.TrxId(1), undo.RecordTypeInsert, insertUndo)
		v1updated, _ := NewPrimaryRecord(env.iter.ct, env.iter.bp, NewPrimaryRecordInput{
			fileId: page.FileId(2), pkCount: 1, lastTrxId: lock.TrxId(1), rollPtr: ptr1,
			colNames: []string{"id", "name", "email"}, values: []string{"1", "Alice", "a@example.com"},
		})
		mtr1 := buffer.NewMtr(env.iter.bp)
		_ = env.iter.primaryTree.Update(mtr1, v1updated.Encode())
		mtr1.UnpinAll()

		// v2: trx=3 UPDATE (Bob)
		v2 := newTestPrimaryRecord(t, env.iter, "1", "Bob", "a@example.com", lock.TrxId(3), undo.NullPointer())
		updateUndo1 := undo.NewUpdateRecord(page.FileId(2), v1updated.Encode(), v2.Encode(), lock.TrxId(1), ptr1)
		ptr2 := env.appendUndo(t, lock.TrxId(3), undo.RecordTypeUpdate, updateUndo1)
		v2.setRollPtr(ptr2)
		mtr2 := buffer.NewMtr(env.iter.bp)
		_ = env.iter.primaryTree.Update(mtr2, v2.Encode())
		mtr2.UnpinAll()

		// v3: trx=5 UPDATE (Carol)
		v3 := newTestPrimaryRecord(t, env.iter, "1", "Carol", "a@example.com", lock.TrxId(5), undo.NullPointer())
		updateUndo2 := undo.NewUpdateRecord(page.FileId(2), v2.Encode(), v3.Encode(), lock.TrxId(3), ptr2)
		ptr3 := env.appendUndo(t, lock.TrxId(5), undo.RecordTypeUpdate, updateUndo2)
		v3.setRollPtr(ptr3)
		mtr3 := buffer.NewMtr(env.iter.bp)
		_ = env.iter.primaryTree.Update(mtr3, v3.Encode())
		mtr3.UnpinAll()

		// trx=4 の ReadView: trx=5 が unique で不可視 (lowLimitId=5)。trx=3 は完了 (active 外)
		rv := newReadView(lock.TrxId(4), nil, lock.TrxId(5))
		iter, mtr := searchPrimaryIndexWithReadView(t, env, rv)
		defer mtr.UnpinAll()

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Bob", result.values[1])
	})
}

// mvccTestEnv は MVCC テスト用の環境
type mvccTestEnv struct {
	iter    *iteratorTestEnv
	undoLog *undo.Manager
}

func (e *mvccTestEnv) appendUndo(t *testing.T, trxId lock.TrxId, recordType undo.RecordType, record undo.Record) undo.Pointer {
	t.Helper()
	ptr, err := e.undoLog.Append(trxId, recordType, record)
	if err != nil {
		t.Fatalf("undo Append に失敗: %v", err)
	}
	return ptr
}

// setupMVCCTestEnv は MVCC テスト用の環境を構築する
func setupMVCCTestEnv(t *testing.T) *mvccTestEnv {
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
	undoMgr, err := undo.NewManager(undoBootstrapMtr, page.FileId(3))
	if err != nil {
		undoBootstrapMtr.UnpinAll()
		t.Fatalf("undo.Manager の作成に失敗: %v", err)
	}
	if err := undoBootstrapMtr.Commit(); err != nil {
		t.Fatalf("undo.Manager Commit に失敗: %v", err)
	}
	return &mvccTestEnv{iter: iter, undoLog: undoMgr}
}

// insertPrimaryRecordWithMvcc は MVCC 情報付きでレコードを挿入し、構築した PrimaryRecord を返す
func insertPrimaryRecordWithMvcc(t *testing.T, env *mvccTestEnv, trxId lock.TrxId, rollPtr undo.Pointer, id, name, email string) *PrimaryRecord {
	t.Helper()
	pr, err := NewPrimaryRecord(env.iter.ct, env.iter.bp, NewPrimaryRecordInput{
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
	mtr := buffer.NewMtr(env.iter.bp)
	defer mtr.UnpinAll()
	if err := env.iter.primaryTree.Insert(mtr, pr.Encode()); err != nil {
		t.Fatalf("プライマリレコードの挿入に失敗: %v", err)
	}
	return pr
}

// updatePrimaryRecordWithMvcc は MVCC 情報付きでレコードを Update し、新しい PrimaryRecord を返す
func updatePrimaryRecordWithMvcc(t *testing.T, env *mvccTestEnv, trxId lock.TrxId, rollPtr undo.Pointer, id, name, email string) *PrimaryRecord {
	t.Helper()
	pr, err := NewPrimaryRecord(env.iter.ct, env.iter.bp, NewPrimaryRecordInput{
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
	mtr := buffer.NewMtr(env.iter.bp)
	defer mtr.UnpinAll()
	if err := env.iter.primaryTree.Update(mtr, pr.Encode()); err != nil {
		t.Fatalf("プライマリレコードの更新に失敗: %v", err)
	}
	return pr
}

// searchPrimaryIndexWithReadView は readView 付きでイテレータと mtr を返す
//   - 呼び出し側は defer mtr.UnpinAll() で解放する
func searchPrimaryIndexWithReadView(t *testing.T, env *mvccTestEnv, rv *readView) (*PrimaryIndexIterator, *buffer.Mtr) {
	t.Helper()
	mtr := buffer.NewMtr(env.iter.bp)
	iter, err := env.iter.primaryTree.Search(mtr, SearchModeStart{}.Encode())
	if err != nil {
		mtr.UnpinAll()
		t.Fatalf("プライマリインデックスの検索に失敗: %v", err)
	}
	return NewPrimaryIndexIterator(iter, env.iter.ct, env.iter.bp, page.FileId(2), rv, env.undoLog), mtr
}

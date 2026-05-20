package undo

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewManager(t *testing.T) {
	t.Run("Manager を作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupTestBufferPool(t)

		// WHEN
		mgr, err := NewManager(bp, page.FileId(1))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, mgr)
	})
}

func TestManagerAppend(t *testing.T) {
	t.Run("Undo レコードを追加すると Pointer を返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		record := NewInsertRecord(page.FileId(1), node.Record{[]byte("Alice")})

		// WHEN
		ptr, err := mgr.Append(lock.TrxId(1), RecordTypeInsert, record)

		// THEN
		assert.NoError(t, err)
		assert.False(t, ptr.IsNull())
	})

	t.Run("同一トランザクションに複数レコードを追加できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), node.Record{[]byte("Alice")})
		r2 := NewDeleteRecord(page.FileId(1), node.Record{[]byte("Bob")}, 1, NullPointer)

		// WHEN
		ptr1, err1 := mgr.Append(lock.TrxId(1), RecordTypeInsert, r1)
		ptr2, err2 := mgr.Append(lock.TrxId(1), RecordTypeDelete, r2)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.NotEqual(t, ptr1, ptr2)
	})

	t.Run("異なるトランザクションにレコードを追加できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), node.Record{[]byte("Alice")})
		r2 := NewInsertRecord(page.FileId(1), node.Record{[]byte("Bob")})

		// WHEN
		_, err1 := mgr.Append(lock.TrxId(1), RecordTypeInsert, r1)
		_, err2 := mgr.Append(lock.TrxId(2), RecordTypeInsert, r2)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.Len(t, mgr.Records(lock.TrxId(1)), 1)
		assert.Len(t, mgr.Records(lock.TrxId(2)), 1)
	})
}

func TestManagerRecords(t *testing.T) {
	t.Run("追加したレコードを取得できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewInsertRecord(page.FileId(1), node.Record{[]byte("Alice")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r)

		// WHEN
		records := mgr.Records(lock.TrxId(1))

		// THEN
		assert.Len(t, records, 1)
	})

	t.Run("複数レコードを追加順に取得できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), node.Record{[]byte("first")})
		r2 := NewDeleteRecord(page.FileId(1), node.Record{[]byte("second")}, 1, NullPointer)
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeDelete, r2)

		// WHEN
		records := mgr.Records(lock.TrxId(1))

		// THEN
		assert.Len(t, records, 2)
	})

	t.Run("レコードが存在しないトランザクションは nil を返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN
		records := mgr.Records(lock.TrxId(999))

		// THEN
		assert.Nil(t, records)
	})

	t.Run("別トランザクションのレコードは返さない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewInsertRecord(page.FileId(1), node.Record{[]byte("Alice")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r)

		// WHEN
		records := mgr.Records(lock.TrxId(2))

		// THEN
		assert.Nil(t, records)
	})
}

func TestManagerPopLast(t *testing.T) {
	t.Run("最後のレコードが削除される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), node.Record{[]byte("first")})
		r2 := NewInsertRecord(page.FileId(1), node.Record{[]byte("second")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r2)

		// WHEN
		mgr.PopLast(lock.TrxId(1))

		// THEN
		assert.Len(t, mgr.Records(lock.TrxId(1)), 1)
	})

	t.Run("レコードが 1 件の場合は空になる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewInsertRecord(page.FileId(1), node.Record{[]byte("only")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r)

		// WHEN
		mgr.PopLast(lock.TrxId(1))

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
	})

	t.Run("レコードがないトランザクションに対しては何もしない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN (パニックしないことを確認)
		mgr.PopLast(lock.TrxId(999))

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(999)))
	})
}

func TestManagerDiscard(t *testing.T) {
	t.Run("指定トランザクションのレコードがすべて破棄される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), node.Record{[]byte("first")})
		r2 := NewInsertRecord(page.FileId(1), node.Record{[]byte("second")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r2)

		// WHEN
		mgr.Discard(lock.TrxId(1))

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
	})

	t.Run("別トランザクションのレコードには影響しない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), node.Record{[]byte("trx1")})
		r2 := NewInsertRecord(page.FileId(1), node.Record{[]byte("trx2")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = mgr.Append(lock.TrxId(2), RecordTypeInsert, r2)

		// WHEN
		mgr.Discard(lock.TrxId(1))

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
		assert.Len(t, mgr.Records(lock.TrxId(2)), 1)
	})

	t.Run("レコードがないトランザクションに対しては何もしない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN (パニックしないことを確認)
		mgr.Discard(lock.TrxId(999))

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(999)))
	})
}

// setupTestBufferPool はテスト用の BufferPool を作成する (Undo ファイル用 FileId=1)
func setupTestBufferPool(t *testing.T) *buffer.BufferPool {
	t.Helper()
	undoPath := filepath.Join(t.TempDir(), "undo.db")
	hf, err := file.NewHeapFile(page.FileId(1), undoPath)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })

	bp := buffer.NewBufferPool(page.PageSize * 20)
	bp.RegisterHeapFile(page.FileId(1), hf)
	return bp
}

// setupTestManager はテスト用の Manager を作成する
func setupTestManager(t *testing.T) *Manager {
	t.Helper()
	bp := setupTestBufferPool(t)
	mgr, err := NewManager(bp, page.FileId(1))
	if err != nil {
		t.Fatalf("Manager の作成に失敗: %v", err)
	}
	return mgr
}

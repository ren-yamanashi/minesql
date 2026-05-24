package undo

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
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
		mgr, err := NewManager(bp, nil, page.FileId(1))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, mgr)
	})
}

func TestManagerAppend(t *testing.T) {
	t.Run("Undo レコードを追加すると Pointer を返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		record := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})

		// WHEN
		ptr, err := mgr.Append(lock.TrxId(1), RecordTypeInsert, record)

		// THEN
		assert.NoError(t, err)
		assert.False(t, ptr.IsNull())
	})

	t.Run("同一トランザクションに複数レコードを追加できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})
		r2 := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("Bob")}, 1, NullPointer())

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
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Bob")})

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
		r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r)

		// WHEN
		records := mgr.Records(lock.TrxId(1))

		// THEN
		assert.Len(t, records, 1)
	})

	t.Run("複数レコードを追加順に取得できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("first")})
		r2 := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("second")}, 1, NullPointer())
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
		r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r)

		// WHEN
		records := mgr.Records(lock.TrxId(2))

		// THEN
		assert.Nil(t, records)
	})
}

func TestManagerCommittedEntries(t *testing.T) {
	t.Run("指定したトランザクションのエントリを返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("a")}, 1, NullPointer())
		r2 := NewUpdateRecord(page.FileId(1), btree.Record{[]byte("old")}, btree.Record{[]byte("new")}, 1, NullPointer())
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeDelete, r1)
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeUpdate, r2)

		// WHEN
		entries := mgr.CommittedEntries([]lock.TrxId{1})

		// THEN
		assert.Len(t, entries, 2)
		assert.Equal(t, lock.TrxId(1), entries[0].TrxId())
		assert.Equal(t, RecordTypeDelete, entries[0].RecordType())
		assert.Equal(t, lock.TrxId(1), entries[1].TrxId())
		assert.Equal(t, RecordTypeUpdate, entries[1].RecordType())
	})

	t.Run("複数トランザクションのエントリをまとめて返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("a")}, 1, NullPointer())
		r2 := NewUpdateRecord(page.FileId(1), btree.Record{[]byte("old")}, btree.Record{[]byte("new")}, 2, NullPointer())
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeDelete, r1)
		_, _ = mgr.Append(lock.TrxId(2), RecordTypeUpdate, r2)

		// WHEN
		entries := mgr.CommittedEntries([]lock.TrxId{1, 2})

		// THEN
		assert.Len(t, entries, 2)
		assert.Equal(t, lock.TrxId(1), entries[0].TrxId())
		assert.Equal(t, lock.TrxId(2), entries[1].TrxId())
	})

	t.Run("指定していないトランザクションのエントリは含まれない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("a")}, 1, NullPointer())
		r2 := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("b")}, 2, NullPointer())
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeDelete, r1)
		_, _ = mgr.Append(lock.TrxId(2), RecordTypeDelete, r2)

		// WHEN
		entries := mgr.CommittedEntries([]lock.TrxId{1})

		// THEN
		assert.Len(t, entries, 1)
		assert.Equal(t, lock.TrxId(1), entries[0].TrxId())
	})

	t.Run("該当するエントリがない場合空のスライスを返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN
		entries := mgr.CommittedEntries([]lock.TrxId{999})

		// THEN
		assert.Empty(t, entries)
	})

	t.Run("空のトランザクション ID リストでは空のスライスを返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("a")}, 1, NullPointer())
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeDelete, r)

		// WHEN
		entries := mgr.CommittedEntries([]lock.TrxId{})

		// THEN
		assert.Empty(t, entries)
	})
}

func TestManagerDiscard(t *testing.T) {
	t.Run("指定トランザクションのレコードがすべて破棄される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("first")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("second")})
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
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("trx1")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("trx2")})
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

func TestManagerDiscardRecordType(t *testing.T) {
	t.Run("指定したレコードタイプのみ破棄される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("inserted")})
		r2 := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("deleted")}, 1, NullPointer())
		r3 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("inserted2")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeDelete, r2)
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r3)

		// WHEN
		mgr.DiscardRecordType(lock.TrxId(1), RecordTypeInsert)

		// THEN
		records := mgr.Records(lock.TrxId(1))
		assert.Len(t, records, 1)
		_, ok := records[0].(DeleteRecord)
		assert.True(t, ok)
	})

	t.Run("全レコードが対象タイプの場合はエントリ自体が削除される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("b")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r2)

		// WHEN
		mgr.DiscardRecordType(lock.TrxId(1), RecordTypeInsert)

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
	})

	t.Run("対象タイプが存在しない場合はレコードが変わらない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("a")}, 1, NullPointer())
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeDelete, r)

		// WHEN
		mgr.DiscardRecordType(lock.TrxId(1), RecordTypeInsert)

		// THEN
		records := mgr.Records(lock.TrxId(1))
		assert.Len(t, records, 1)
	})

	t.Run("別トランザクションのレコードには影響しない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("trx1")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("trx2")})
		_, _ = mgr.Append(lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = mgr.Append(lock.TrxId(2), RecordTypeInsert, r2)

		// WHEN
		mgr.DiscardRecordType(lock.TrxId(1), RecordTypeInsert)

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
		assert.Len(t, mgr.Records(lock.TrxId(2)), 1)
	})

	t.Run("レコードがないトランザクションに対しては何もしない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN (パニックしないことを確認)
		mgr.DiscardRecordType(lock.TrxId(999), RecordTypeInsert)

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(999)))
	})
}

func TestManagerWriteToPage(t *testing.T) {
	t.Run("ページが満杯になると新しいページに書き込まれる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		// 大きなレコードを作成してページを埋める
		bigData := make([]byte, 1000)
		for i := range bigData {
			bigData[i] = byte(i % 256)
		}

		// WHEN (ページサイズを超えるまで書き込む)
		var lastPtr Pointer
		var err error
		for i := range 20 {
			r := NewInsertRecord(page.FileId(1), btree.Record{bigData})
			lastPtr, err = mgr.Append(lock.TrxId(1), RecordTypeInsert, r)
			if err != nil {
				t.Fatalf("Append %d に失敗: %v", i, err)
			}
		}

		// THEN
		assert.NoError(t, err)
		// 複数ページにまたがるため、最後の Pointer のページ番号は最初と異なるはず
		assert.NotEqual(t, page.PageNumber(0), lastPtr.pageNumber)
	})
}

// setupTestBufferPool はテスト用の BufferPool を作成する (Undo ファイル用 FileId=1)
func setupTestBufferPool(t *testing.T) *buffer.Pool {
	t.Helper()
	undoPath := filepath.Join(t.TempDir(), "undo.db")
	hf, err := file.NewHeapFile(page.FileId(1), undoPath)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })

	bp := buffer.NewPool(page.Size * 20)
	bp.RegisterHeapFile(page.FileId(1), hf)
	return bp
}

// setupTestManager はテスト用の Manager を作成する
func setupTestManager(t *testing.T) *Manager {
	t.Helper()
	bp := setupTestBufferPool(t)
	mgr, err := NewManager(bp, nil, page.FileId(1))
	if err != nil {
		t.Fatalf("Manager の作成に失敗: %v", err)
	}
	return mgr
}

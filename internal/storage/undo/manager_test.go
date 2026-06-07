package undo

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
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
		ptr, err := appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, record)

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
		ptr1, err1 := appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		ptr2, err2 := appendForTest(t, mgr, lock.TrxId(1), RecordTypeDelete, r2)

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
		_, err1 := appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, err2 := appendForTest(t, mgr, lock.TrxId(2), RecordTypeInsert, r2)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.Len(t, mgr.Records(lock.TrxId(1)), 1)
		assert.Len(t, mgr.Records(lock.TrxId(2)), 1)
	})

	t.Run("複数 goroutine から Append しても map の同時書き込みが起きない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		const numGoroutines = 10
		const appendsPerGoroutine = 20

		// WHEN
		var wg sync.WaitGroup
		for g := range numGoroutines {
			trxId := lock.TrxId(g + 1)
			wg.Go(func() {
				for range appendsPerGoroutine {
					r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("data")})
					_, err := appendForTest(t, mgr, trxId, RecordTypeInsert, r)
					assert.NoError(t, err)
				}
			})
		}
		wg.Wait()

		// THEN
		for g := range numGoroutines {
			records := mgr.Records(lock.TrxId(g + 1))
			assert.Len(t, records, appendsPerGoroutine)
		}
	})

	t.Run("ページ満杯時の switch では新ページ実体化の REDO がリンク変更 REDO より前に出る", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupTestManagerWithRedoLog(t)
		oldPageId := mgr.currentPageId
		rec := NewInsertRecord(page.FileId(1), btree.Record{[]byte("x")})

		// WHEN
		for mgr.currentPageId == oldPageId {
			_, err := appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, rec)
			assert.NoError(t, err)
		}
		newPageId := mgr.currentPageId
		assert.NoError(t, redoLog.Flush())
		records, err := redoLog.ReadFrom(redo.Lsn(0))
		assert.NoError(t, err)
		var pageWrites []redo.Record
		for _, r := range records {
			if r.Type() == redo.RecordTypePageWrite {
				pageWrites = append(pageWrites, r)
			}
		}

		// THEN
		assert.NotEqual(t, oldPageId, newPageId)
		n := len(pageWrites)
		assert.GreaterOrEqual(t, n, 2)
		assert.Equal(t, newPageId, pageWrites[n-2].PageId())
		assert.Equal(t, oldPageId, pageWrites[n-1].PageId())
	})

	t.Run("Append と Discard が並行実行されてもデータレースが起きない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		const numAppenders = 5
		const numDiscarders = 5
		const opsPerGoroutine = 20

		// WHEN
		var wg sync.WaitGroup
		for g := range numAppenders {
			trxId := lock.TrxId(g + 1)
			wg.Go(func() {
				for range opsPerGoroutine {
					r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("data")})
					_, _ = appendForTest(t, mgr, trxId, RecordTypeInsert, r)
				}
			})
		}
		for g := range numDiscarders {
			trxId := lock.TrxId(g + 1)
			wg.Go(func() {
				for range opsPerGoroutine {
					mgr.Discard(trxId)
				}
			})
		}
		wg.Wait()

		// THEN: パニックせず完了すれば OK (-race フラグでレースが検出されないこと)
	})
}

func TestManagerRecords(t *testing.T) {
	t.Run("追加したレコードを取得できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r)

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
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeDelete, r2)

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
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r)

		// WHEN
		records := mgr.Records(lock.TrxId(2))

		// THEN
		assert.Nil(t, records)
	})
}

func TestManagerLookupByPointer(t *testing.T) {
	t.Run("Insert レコードを Pointer から取得できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		original := NewInsertRecord(page.FileId(1), btree.Record{[]byte("alice")})
		ptr, err := appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, original)
		assert.NoError(t, err)

		// WHEN
		got, err := lookupForTest(t, mgr, ptr)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, RecordTypeInsert, got.RecordType())
		assert.Equal(t, lock.TrxId(0), got.PrevLastTrxId())
		assert.True(t, got.PrevRollPtr().IsNull())
		ir, ok := got.(InsertRecord)
		assert.True(t, ok)
		assert.Equal(t, original.Record(), ir.Record())
	})

	t.Run("Update レコードを Pointer から取得できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		prev := btree.Record{[]byte("old")}
		next := btree.Record{[]byte("new")}
		prevTrxId := lock.TrxId(5)
		prevPtr := NewPointer(7, 64)
		original := NewUpdateRecord(page.FileId(1), prev, next, prevTrxId, prevPtr)
		ptr, err := appendForTest(t, mgr, lock.TrxId(10), RecordTypeUpdate, original)
		assert.NoError(t, err)

		// WHEN
		got, err := lookupForTest(t, mgr, ptr)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, RecordTypeUpdate, got.RecordType())
		assert.Equal(t, prevTrxId, got.PrevLastTrxId())
		assert.Equal(t, prevPtr, got.PrevRollPtr())
		ur, ok := got.(UpdateRecord)
		assert.True(t, ok)
		assert.Equal(t, prev, ur.PrevRecord())
		assert.Equal(t, next, ur.NewRecord())
	})

	t.Run("Delete レコードを Pointer から取得できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		row := btree.Record{[]byte("bob")}
		prevTrxId := lock.TrxId(3)
		prevPtr := NewPointer(4, 32)
		original := NewDeleteRecord(page.FileId(1), row, prevTrxId, prevPtr)
		ptr, err := appendForTest(t, mgr, lock.TrxId(8), RecordTypeDelete, original)
		assert.NoError(t, err)

		// WHEN
		got, err := lookupForTest(t, mgr, ptr)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, RecordTypeDelete, got.RecordType())
		assert.Equal(t, prevTrxId, got.PrevLastTrxId())
		assert.Equal(t, prevPtr, got.PrevRollPtr())
		dr, ok := got.(DeleteRecord)
		assert.True(t, ok)
		assert.Equal(t, row, dr.Record())
	})

	t.Run("NullPointer を渡すと ErrNullPointer", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN
		_, err := lookupForTest(t, mgr, NullPointer())

		// THEN
		assert.ErrorIs(t, err, ErrNullPointer)
	})

	t.Run("同一ページに複数レコードを書いてそれぞれ正しく取得できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("first")})
		r2 := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("second")}, lock.TrxId(2), NullPointer())
		ptr1, err := appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		assert.NoError(t, err)
		ptr2, err := appendForTest(t, mgr, lock.TrxId(1), RecordTypeDelete, r2)
		assert.NoError(t, err)
		assert.NotEqual(t, ptr1, ptr2)

		// WHEN
		got1, err1 := lookupForTest(t, mgr, ptr1)
		got2, err2 := lookupForTest(t, mgr, ptr2)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.Equal(t, RecordTypeInsert, got1.RecordType())
		assert.Equal(t, RecordTypeDelete, got2.RecordType())
	})

	t.Run("ページ切替後のレコードも取得できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		bigData := make([]byte, 1000)
		oldPageId := mgr.currentPageId
		var lastPtr Pointer
		var lastRecord InsertRecord
		for mgr.currentPageId == oldPageId {
			lastRecord = NewInsertRecord(page.FileId(1), btree.Record{bigData})
			ptr, err := appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, lastRecord)
			assert.NoError(t, err)
			lastPtr = ptr
		}
		assert.NotEqual(t, oldPageId, mgr.currentPageId)

		// WHEN
		got, err := lookupForTest(t, mgr, lastPtr)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, RecordTypeInsert, got.RecordType())
		ir, ok := got.(InsertRecord)
		assert.True(t, ok)
		assert.Equal(t, lastRecord.Record(), ir.Record())
	})
}

func TestManagerCommittedEntries(t *testing.T) {
	t.Run("指定したトランザクションのエントリを返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("a")}, 1, NullPointer())
		r2 := NewUpdateRecord(page.FileId(1), btree.Record{[]byte("old")}, btree.Record{[]byte("new")}, 1, NullPointer())
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeDelete, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeUpdate, r2)

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
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeDelete, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(2), RecordTypeUpdate, r2)

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
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeDelete, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(2), RecordTypeDelete, r2)

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
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeDelete, r)

		// WHEN
		entries := mgr.CommittedEntries([]lock.TrxId{})

		// THEN
		assert.Empty(t, entries)
	})

	t.Run("CommittedEntries と Append が並行実行されてもデータレースが起きない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		const numGoroutines = 10
		const opsPerGoroutine = 20
		trxIds := make([]lock.TrxId, numGoroutines)
		for i := range trxIds {
			trxIds[i] = lock.TrxId(i + 1)
		}

		// WHEN
		var wg sync.WaitGroup
		for g := range numGoroutines {
			trxId := lock.TrxId(g + 1)
			wg.Go(func() {
				for range opsPerGoroutine {
					r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("data")})
					_, _ = appendForTest(t, mgr, trxId, RecordTypeInsert, r)
				}
			})
		}
		for range numGoroutines {
			wg.Go(func() {
				for range opsPerGoroutine {
					_ = mgr.CommittedEntries(trxIds)
				}
			})
		}
		wg.Wait()

		// THEN: パニックせず完了すれば OK
	})
}

func TestManagerDiscard(t *testing.T) {
	t.Run("指定トランザクションのレコードがすべて破棄される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("first")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("second")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r2)

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
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(2), RecordTypeInsert, r2)

		// WHEN
		mgr.Discard(lock.TrxId(1))

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
		assert.Len(t, mgr.Records(lock.TrxId(2)), 1)
	})

	t.Run("レコードがないトランザクションに対しては何もしない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN
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
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeDelete, r2)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r3)

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
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r2)

		// WHEN
		mgr.DiscardRecordType(lock.TrxId(1), RecordTypeInsert)

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
	})

	t.Run("対象タイプが存在しない場合はレコードが変わらない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("a")}, 1, NullPointer())
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeDelete, r)

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
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(2), RecordTypeInsert, r2)

		// WHEN
		mgr.DiscardRecordType(lock.TrxId(1), RecordTypeInsert)

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
		assert.Len(t, mgr.Records(lock.TrxId(2)), 1)
	})

	t.Run("レコードがないトランザクションに対しては何もしない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN
		mgr.DiscardRecordType(lock.TrxId(999), RecordTypeInsert)

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(999)))
	})
}

func TestManagerWriteToPage(t *testing.T) {
	t.Run("ページが満杯になると新しいページに書き込まれる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		bigData := make([]byte, 1000)
		for i := range bigData {
			bigData[i] = byte(i % 256)
		}

		// WHEN
		var lastPtr Pointer
		var err error
		for i := range 20 {
			r := NewInsertRecord(page.FileId(1), btree.Record{bigData})
			lastPtr, err = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r)
			if err != nil {
				t.Fatalf("Append %d に失敗: %v", i, err)
			}
		}

		// THEN
		assert.NoError(t, err)
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

	bp := buffer.NewPool(page.Size*20, nil)
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

// setupTestManagerWithRedoLog はテスト用の Manager を実 redoLog 付きで作成する
//   - REDO ログの内容を検証したいテスト用
func setupTestManagerWithRedoLog(t *testing.T) (*Manager, *redo.Buffer) {
	t.Helper()
	bp := setupTestBufferPool(t)
	redoLog, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Close() })
	mgr, err := NewManager(bp, redoLog, page.FileId(1))
	if err != nil {
		t.Fatalf("Manager の作成に失敗: %v", err)
	}
	return mgr, redoLog
}

// appendForTest は 1 回の Append を独立した mtr スコープで実行するヘルパー
//   - Manager の Append は呼び出し側 (= access) の mtr を引き継ぐ設計のため、テストでは 1 件単位で mtr を生成 / 解放する
func appendForTest(
	t *testing.T,
	mgr *Manager,
	trxId lock.TrxId,
	recordType RecordType,
	record Record,
) (Pointer, error) {
	t.Helper()
	mtr := buffer.NewMtr(mgr.bufferPool)
	defer mtr.UnpinAll()
	return mgr.Append(mtr, trxId, recordType, record)
}

// lookupForTest は LookupByPointer を独立した mtr スコープで実行するヘルパー
func lookupForTest(t *testing.T, mgr *Manager, ptr Pointer) (Record, error) {
	t.Helper()
	mtr := buffer.NewMtr(mgr.bufferPool)
	defer mtr.UnpinAll()
	return mgr.LookupByPointer(mtr, ptr)
}

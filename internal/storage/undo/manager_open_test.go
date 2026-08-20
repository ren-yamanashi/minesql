package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestOpenManagerEmpty(t *testing.T) {
	t.Run("レコードがない既存ファイルを開くと entries は空", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN
		opened, err := OpenManager(mgr.bufferPool, mgr.fileId)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, opened.entries)
		assert.Equal(t, page.NewId(mgr.fileId, ChainHeadPageNumber), opened.currentPageId)
	})
}

func TestOpenManagerRestoreSingleTrx(t *testing.T) {
	t.Run("単一 trxId の Insert レコードを復元できる", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		record := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})
		_, err := appendForTest(t, mgr, lock.TrxId(10), RecordTypeInsert, record)
		assert.NoError(t, err)

		// WHEN
		opened, err := OpenManager(mgr.bufferPool, mgr.fileId)

		// THEN
		assert.NoError(t, err)
		entries := opened.entries[lock.TrxId(10)]
		assert.Len(t, entries, 1)
		assert.Equal(t, RecordTypeInsert, entries[0].RecordType())
		assert.Equal(t, lock.TrxId(10), entries[0].TrxId())
	})
}

func TestOpenManagerRestoreMultipleTrx(t *testing.T) {
	t.Run("複数 trxId の Undo レコードを trxId 単位で entries に積み直す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		recordA := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})
		recordB := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Bob")})
		_, err := appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, recordA)
		assert.NoError(t, err)
		_, err = appendForTest(t, mgr, lock.TrxId(2), RecordTypeInsert, recordB)
		assert.NoError(t, err)
		_, err = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, recordB)
		assert.NoError(t, err)

		// WHEN
		opened, err := OpenManager(mgr.bufferPool, mgr.fileId)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, opened.entries[lock.TrxId(1)], 2)
		assert.Len(t, opened.entries[lock.TrxId(2)], 1)
	})
}

func TestHistoryTrxIds(t *testing.T) {
	t.Run("Undo を持たない Manager は空集合を返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN
		ids := mgr.HistoryTrxIds()

		// THEN
		assert.Empty(t, ids)
	})

	t.Run("UPDATE / DELETE 種別の Undo を持つ trxId が収集される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		deleteRec := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("Alice")}, 0, NullPointer())
		updateRec := NewUpdateRecord(
			page.FileId(1),
			btree.Record{[]byte("Bob")},
			btree.Record{[]byte("Bobby")},
			0, NullPointer(),
		)
		_, err := appendForTest(t, mgr, lock.TrxId(10), RecordTypeDelete, deleteRec)
		assert.NoError(t, err)
		_, err = appendForTest(t, mgr, lock.TrxId(20), RecordTypeUpdate, updateRec)
		assert.NoError(t, err)
		opened, err := OpenManager(mgr.bufferPool, mgr.fileId)
		assert.NoError(t, err)

		// WHEN
		ids := opened.HistoryTrxIds()

		// THEN
		assert.ElementsMatch(t, []lock.TrxId{10, 20}, ids)
	})

	t.Run("INSERT 種別のみを持つ trxId は収集されない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		insertRec := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})
		_, err := appendForTest(t, mgr, lock.TrxId(30), RecordTypeInsert, insertRec)
		assert.NoError(t, err)
		opened, err := OpenManager(mgr.bufferPool, mgr.fileId)
		assert.NoError(t, err)

		// WHEN
		ids := opened.HistoryTrxIds()

		// THEN
		assert.Empty(t, ids)
	})

	t.Run("UPDATE と INSERT を併せ持つ trxId は 1 回だけ収集される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		insertRec := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})
		updateRec := NewUpdateRecord(
			page.FileId(1),
			btree.Record{[]byte("Bob")},
			btree.Record{[]byte("Bobby")},
			0, NullPointer(),
		)
		_, err := appendForTest(t, mgr, lock.TrxId(40), RecordTypeInsert, insertRec)
		assert.NoError(t, err)
		_, err = appendForTest(t, mgr, lock.TrxId(40), RecordTypeUpdate, updateRec)
		assert.NoError(t, err)
		opened, err := OpenManager(mgr.bufferPool, mgr.fileId)
		assert.NoError(t, err)

		// WHEN
		ids := opened.HistoryTrxIds()

		// THEN
		assert.Equal(t, []lock.TrxId{40}, ids)
	})
}

func TestOpenManagerCurrentPageIdIsLastPage(t *testing.T) {
	t.Run("currentPageId は最後の Undo ページを指す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		largeData := make([]byte, 3000)
		for i := range largeData {
			largeData[i] = 'a'
		}
		largeRecord := NewInsertRecord(page.FileId(1), btree.Record{largeData})
		for range 5 {
			_, err := appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, largeRecord)
			assert.NoError(t, err)
		}
		startPageId := page.NewId(mgr.fileId, ChainHeadPageNumber)
		assert.NotEqual(t, startPageId, mgr.currentPageId, "事前条件: 新規ページが割り当てられている")

		// WHEN
		opened, err := OpenManager(mgr.bufferPool, mgr.fileId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, mgr.currentPageId, opened.currentPageId)
	})
}

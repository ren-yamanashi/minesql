package undo

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	t.Run("Manager を作成できる", func(t *testing.T) {
		// GIVEN
		redoLog, err := redo.NewBuffer(t.TempDir())
		assert.NoError(t, err)
		t.Cleanup(func() { _ = redoLog.Close() })
		bp := setupTestBufferPool(t, redoLog)

		// WHEN
		openMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		mgr := NewManager(openMtr, page.FileId(1))
		_ = openMtr.Commit()

		// THEN
		assert.NotNil(t, mgr)
	})

	t.Run("チェーン先頭ページの PageNumber は ChainHeadPageNumber", func(t *testing.T) {
		// GIVEN
		redoLog, err := redo.NewBuffer(t.TempDir())
		assert.NoError(t, err)
		t.Cleanup(func() { _ = redoLog.Close() })
		bp := setupTestBufferPool(t, redoLog)

		// WHEN
		openMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		mgr := NewManager(openMtr, page.FileId(1))
		_ = openMtr.Commit()

		// THEN
		assert.Equal(t, ChainHeadPageNumber, mgr.currentPageId.PageNumber())
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

	t.Run("Append は独立した mini-transaction として MtrStart + PageWrite + MtrEnd を REDO に記録する", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupTestManagerWithRedoLog(t)
		assert.NoError(t, redoLog.Flush())
		baseLsn := redoLog.FlushedLsn()
		rec := NewInsertRecord(page.FileId(1), btree.Record{[]byte("data")})

		// WHEN
		_, err := mgr.Append(lock.TrxId(1), RecordTypeInsert, rec)
		assert.NoError(t, err)

		// THEN
		assert.NoError(t, redoLog.Flush())
		records, err := redoLog.ReadFrom(baseLsn)
		assert.NoError(t, err)
		var types []redo.RecordType
		for _, r := range records {
			types = append(types, r.Type())
		}
		assert.Equal(t, []redo.RecordType{
			redo.RecordTypeMtrStart,
			redo.RecordTypePageWrite,
			redo.RecordTypeMtrEnd,
		}, types)
	})

	t.Run("Append の REDO は後続のデータ操作 mini-transaction の REDO より前に位置する", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupTestManagerWithRedoLog(t)
		dataFileId := page.FileId(2)
		dataPath := filepath.Join(t.TempDir(), "data.db")
		dataHf, err := file.NewHeapFile(dataPath)
		assert.NoError(t, err)
		t.Cleanup(func() { _ = dataHf.Close() })
		mgr.bufferPool.RegisterHeapFile(dataFileId, dataHf)
		initMtr := buffer.NewWriteMtr(mgr.bufferPool, lock.SystemReservedTrxId, redoLog)
		assert.NoError(t, fsp.InitHeader(initMtr, dataFileId))
		assert.NoError(t, initMtr.Commit())
		allocMtr := buffer.NewWriteMtr(mgr.bufferPool, lock.SystemReservedTrxId, redoLog)
		dataPageId, err := fsp.AllocatePage(allocMtr, dataFileId)
		assert.NoError(t, err)
		assert.NoError(t, allocMtr.Commit())
		_, err = mgr.bufferPool.AddPage(dataPageId)
		assert.NoError(t, err)
		assert.NoError(t, redoLog.Flush())
		baseLsn := redoLog.FlushedLsn()
		rec := NewInsertRecord(page.FileId(1), btree.Record{[]byte("x")})

		// WHEN
		_, err = mgr.Append(lock.TrxId(1), RecordTypeInsert, rec)
		assert.NoError(t, err)
		dataMtr := buffer.NewWriteMtr(mgr.bufferPool, lock.TrxId(1), redoLog)
		dataPage, err := dataMtr.PageForWrite(dataPageId)
		assert.NoError(t, err)
		dataPage.WriteBodyAt(0, []byte{0xFF})
		assert.NoError(t, dataMtr.Commit())

		// THEN
		assert.NoError(t, redoLog.Flush())
		records, err := redoLog.ReadFrom(baseLsn)
		assert.NoError(t, err)
		var firstUndoLsn, firstDataLsn redo.Lsn
		for _, r := range records {
			if r.Type() != redo.RecordTypePageWrite {
				continue
			}
			if r.PageId() == dataPageId && firstDataLsn == 0 {
				firstDataLsn = r.Lsn()
			}
			if r.PageId().FileId() == page.FileId(1) && firstUndoLsn == 0 {
				firstUndoLsn = r.Lsn()
			}
		}
		assert.NotZero(t, firstUndoLsn)
		assert.NotZero(t, firstDataLsn)
		assert.Less(t, firstUndoLsn, firstDataLsn)
	})

	t.Run("ページ満杯時の switch では新ページ実体化の REDO がリンク変更 REDO より前に出る", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupTestManagerWithRedoLog(t)
		oldPageId := mgr.currentPageId
		rec := NewInsertRecord(page.FileId(1), btree.Record{[]byte("x")})

		// WHEN: Append を繰り返し、 ページが切り替わるまで埋める
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

		// THEN: 切替を起こした Commit が新ページ→旧ページの順で記録する
		//       (2 つの間には fsp ヘッダー更新の記録が挟まる)
		assert.NotEqual(t, oldPageId, newPageId)
		assert.GreaterOrEqual(t, len(pageWrites), 2)
		newIdx, oldIdx := -1, -1
		for i, r := range pageWrites {
			if r.PageId() == newPageId {
				newIdx = i
			}
			if r.PageId() == oldPageId {
				oldIdx = i
			}
		}
		assert.NotEqual(t, -1, newIdx)
		assert.NotEqual(t, -1, oldIdx)
		assert.Less(t, newIdx, oldIdx)
	})

	t.Run("Append が ErrRecordTooLarge で失敗してもラッチと Pin がリークしない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		oldPageId := mgr.currentPageId
		huge := make([]byte, page.Size)
		hugeRec := NewInsertRecord(page.FileId(1), btree.Record{huge})

		// WHEN
		_, err := mgr.Append(lock.TrxId(1), RecordTypeInsert, hugeRec)

		// THEN
		assert.ErrorIs(t, err, ErrRecordTooLarge)
		probe := buffer.NewMtr(mgr.bufferPool)
		_, perr := probe.PageForWrite(oldPageId)
		assert.NoError(t, perr)
		assert.Equal(t, 1, probe.PinnedCount())
		probe.UnpinAll()
		smallRec := NewInsertRecord(page.FileId(1), btree.Record{[]byte("small")})
		_, err = mgr.Append(lock.TrxId(1), RecordTypeInsert, smallRec)
		assert.NoError(t, err)
	})

	t.Run("上限超過レコードの Append は書き込み前に ErrRecordTooLarge を返し redo に何も記録しない", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupTestManagerWithRedoLog(t)
		require.NoError(t, redoLog.Flush())
		sizeBefore, err := redoLog.Size()
		require.NoError(t, err)
		huge := make([]byte, page.Size)
		hugeRec := NewInsertRecord(page.FileId(1), btree.Record{huge})

		// WHEN
		_, err = mgr.Append(lock.TrxId(1), RecordTypeInsert, hugeRec)

		// THEN
		assert.ErrorIs(t, err, ErrRecordTooLarge)
		require.NoError(t, redoLog.Flush())
		sizeAfter, err := redoLog.Size()
		require.NoError(t, err)
		assert.Equal(t, sizeBefore, sizeAfter)
	})

	t.Run("switchToNewPage で新ページ AddPage が失敗すると割り当てが補償解放されて free に戻る", func(t *testing.T) {
		// GIVEN: fsp 関連ページをプールにロードした後、次に払い出されるページ番号を割り当て → 即解放で予測し、
		//        プールの残り空きスロットをダミー pin で埋めることで、次の Append の AddPage が決定的に失敗する状況を作る
		mgr, redoLog := setupCompensationTestManager(t)
		rec := NewInsertRecord(page.FileId(1), btree.Record{[]byte("x")})
		for range 4 {
			fillUntilPageSwitchInManager(t, mgr, rec)
		}
		fillCurrentPageAlmostFull(t, mgr, rec)
		predictMtr := buffer.NewWriteMtr(mgr.bufferPool, lock.TrxId(1), redoLog)
		predictedId, err := fsp.AllocateSegmentPage(predictMtr, page.FileId(1), chainRootHeaderAt(page.NewId(page.FileId(1), ChainHeadPageNumber)))
		require.NoError(t, err)
		require.NoError(t, fsp.FreeSegmentPage(predictMtr, chainRootHeaderAt(page.NewId(page.FileId(1), ChainHeadPageNumber)), predictedId))
		require.NoError(t, predictMtr.Commit())
		require.NoError(t, redoLog.Flush())
		protected := pinAllWrittenPages(t, mgr.bufferPool, redoLog)
		dummies := fillPoolWithDummyPages(t, mgr.bufferPool)
		sizeBefore, err := redoLog.Size()
		require.NoError(t, err)

		// WHEN
		_, appendErr := mgr.Append(lock.TrxId(1), RecordTypeInsert, rec)

		// THEN
		require.ErrorIs(t, appendErr, buffer.ErrAllPagesUnevictable)
		unpinDummyPages(mgr.bufferPool, dummies)
		unpinDummyPages(mgr.bufferPool, protected)
		sizeAfter, err := redoLog.Size()
		require.NoError(t, err)
		assert.Greater(t, sizeAfter, sizeBefore, "割り当てと補償解放を含む mini-transaction が redo に記録されているはず")
		readMtr := buffer.NewMtr(mgr.bufferPool)
		defer readMtr.UnpinAll()
		isFree, err := fsp.IsPageFree(readMtr, predictedId)
		require.NoError(t, err)
		assert.True(t, isFree, "補償解放によって %v は free に戻っているはず", predictedId)
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

func TestManagerCount(t *testing.T) {
	t.Run("レコードがない場合は 0 を返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN
		count := mgr.Count(lock.TrxId(1))

		// THEN
		assert.Equal(t, 0, count)
	})

	t.Run("Append した件数を返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("b")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r2)

		// WHEN
		count := mgr.Count(lock.TrxId(1))

		// THEN
		assert.Equal(t, 2, count)
	})

	t.Run("別トランザクションのレコードは含まない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r)

		// WHEN
		count := mgr.Count(lock.TrxId(2))

		// THEN
		assert.Equal(t, 0, count)
	})
}

func TestManagerRecordsFrom(t *testing.T) {
	t.Run("from 位置以降のレコードを返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("b")})
		r3 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("c")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		savepoint := mgr.Count(lock.TrxId(1))
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r2)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r3)

		// WHEN
		records := mgr.RecordsFrom(lock.TrxId(1), savepoint)

		// THEN
		assert.Len(t, records, 2)
	})

	t.Run("from が現在件数と等しい場合は nil", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r)
		savepoint := mgr.Count(lock.TrxId(1))

		// WHEN
		records := mgr.RecordsFrom(lock.TrxId(1), savepoint)

		// THEN
		assert.Nil(t, records)
	})

	t.Run("from が現在件数より大きい場合は nil", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)

		// WHEN
		records := mgr.RecordsFrom(lock.TrxId(1), 100)

		// THEN
		assert.Nil(t, records)
	})

	t.Run("from = 0 で全件返す", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("b")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r2)

		// WHEN
		records := mgr.RecordsFrom(lock.TrxId(1), 0)

		// THEN
		assert.Len(t, records, 2)
	})
}

func TestManagerDiscardFrom(t *testing.T) {
	t.Run("from 位置以降のレコードのみ破棄される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("b")})
		r3 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("c")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		savepoint := mgr.Count(lock.TrxId(1))
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r2)
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r3)

		// WHEN
		mgr.DiscardFrom(lock.TrxId(1), savepoint)

		// THEN
		assert.Equal(t, 1, mgr.Count(lock.TrxId(1)))
	})

	t.Run("from = 0 で全件破棄されエントリ自体が削除される", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r)

		// WHEN
		mgr.DiscardFrom(lock.TrxId(1), 0)

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
	})

	t.Run("from が現在件数と等しい場合は no-op", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r)
		savepoint := mgr.Count(lock.TrxId(1))

		// WHEN
		mgr.DiscardFrom(lock.TrxId(1), savepoint)

		// THEN
		assert.Equal(t, 1, mgr.Count(lock.TrxId(1)))
	})

	t.Run("別トランザクションのレコードには影響しない", func(t *testing.T) {
		// GIVEN
		mgr := setupTestManager(t)
		r1 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})
		r2 := NewInsertRecord(page.FileId(1), btree.Record{[]byte("b")})
		_, _ = appendForTest(t, mgr, lock.TrxId(1), RecordTypeInsert, r1)
		_, _ = appendForTest(t, mgr, lock.TrxId(2), RecordTypeInsert, r2)

		// WHEN
		mgr.DiscardFrom(lock.TrxId(1), 0)

		// THEN
		assert.Nil(t, mgr.Records(lock.TrxId(1)))
		assert.Equal(t, 1, mgr.Count(lock.TrxId(2)))
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
func setupTestBufferPool(t *testing.T, redoLog *redo.Buffer) *buffer.Pool {
	t.Helper()
	undoPath := filepath.Join(t.TempDir(), "undo.db")
	hf, err := file.NewHeapFile(undoPath)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })

	bp := buffer.NewPool(page.Size*20, redoLog, nil)
	bp.RegisterHeapFile(page.FileId(1), hf)
	return bp
}

// setupTestManager はテスト用の Manager を作成する
func setupTestManager(t *testing.T) *Manager {
	t.Helper()
	redoLog, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Close() })
	bp := setupTestBufferPool(t, redoLog)
	openMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	mgr := NewManager(openMtr, page.FileId(1))
	if err := openMtr.Commit(); err != nil {
		t.Fatalf("Manager Commit に失敗: %v", err)
	}
	return mgr
}

// setupCompensationTestManager はダミーページで埋める補償テスト用の Manager を作る
//   - fsp が触りうる header / inode / xdes / lease 対象 extent 群と undo ページを十分に載せられる大きめのプール
func setupCompensationTestManager(t *testing.T) (*Manager, *redo.Buffer) {
	t.Helper()
	redoLog, err := redo.NewBuffer(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = redoLog.Close() })
	undoPath := filepath.Join(t.TempDir(), "undo.db")
	hf, err := file.NewHeapFile(undoPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp := buffer.NewPool(page.Size*128, redoLog, nil)
	bp.RegisterHeapFile(page.FileId(1), hf)
	openMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	mgr := NewManager(openMtr, page.FileId(1))
	require.NoError(t, openMtr.Commit())
	return mgr, redoLog
}

// setupTestManagerWithRedoLog はテスト用の Manager と、書き込み Mtr で使う実 redoLog を作成する
//   - Undo ページの変更を Redo 記録する書き込み Mtr の挙動を検証したいテスト用
func setupTestManagerWithRedoLog(t *testing.T) (*Manager, *redo.Buffer) {
	t.Helper()
	redoLog, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Close() })
	bp := setupTestBufferPool(t, redoLog)
	openMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	mgr := NewManager(openMtr, page.FileId(1))
	if err := openMtr.Commit(); err != nil {
		t.Fatalf("Manager Commit に失敗: %v", err)
	}
	return mgr, redoLog
}

// appendForTest は 1 回の Append を実行するヘルパー
func appendForTest(
	t *testing.T,
	mgr *Manager,
	trxId lock.TrxId,
	recordType RecordType,
	record Record,
) (Pointer, error) {
	t.Helper()
	return mgr.Append(trxId, recordType, record)
}

// lookupForTest は LookupByPointer を独立した mtr スコープで実行するヘルパー
func lookupForTest(t *testing.T, mgr *Manager, ptr Pointer) (Record, error) {
	t.Helper()
	mtr := buffer.NewMtr(mgr.bufferPool)
	defer mtr.UnpinAll()
	return mgr.LookupByPointer(mtr, ptr)
}

// fillUntilPageSwitchInManager は現ページが切り替わるまで rec を Append し続ける
func fillUntilPageSwitchInManager(t *testing.T, mgr *Manager, rec Record) {
	t.Helper()
	oldPageId := mgr.currentPageId
	for mgr.currentPageId == oldPageId {
		_, err := mgr.Append(lock.TrxId(1), RecordTypeInsert, rec)
		require.NoError(t, err)
	}
}

// fillCurrentPageAlmostFull は現ページが切り替わる直前まで rec を Append する
//   - 「次の 1 回の Append で切替が発火する」状態を作るため、切替を含まない範囲でループする
func fillCurrentPageAlmostFull(t *testing.T, mgr *Manager, rec Record) {
	t.Helper()
	fixedPageId := mgr.currentPageId
	for {
		bufPage, err := mgr.bufferPool.Page(fixedPageId)
		require.NoError(t, err)
		remaining := openUndoPage(bufPage, fixedPageId).FreeSpace()
		mgr.bufferPool.Unpin(fixedPageId)
		serialized := rec.Serialize(lock.TrxId(1), UndoNumber(len(mgr.entries[lock.TrxId(1)])))
		if remaining < len(serialized) {
			return
		}
		_, err = mgr.Append(lock.TrxId(1), RecordTypeInsert, rec)
		require.NoError(t, err)
		if mgr.currentPageId != fixedPageId {
			t.Fatalf("fillCurrentPageAlmostFull: 切替が想定より早く起きた")
		}
	}
}

// pinAllWrittenPages は事前 Append で書き込まれた PageId (Redo に現れた全て) を pin する
//   - evict retry の flush 経路で clean 化 → 追い出されるのを防ぐため、テスト側で保持する
//   - 呼び出し側は unpinDummyPages で解放する
func pinAllWrittenPages(t *testing.T, bp *buffer.Pool, redoLog *redo.Buffer) []page.Id {
	t.Helper()
	require.NoError(t, redoLog.Flush())
	records, err := redoLog.ReadFrom(redo.Lsn(0))
	require.NoError(t, err)
	seen := make(map[page.Id]bool)
	var ids []page.Id
	for _, r := range records {
		if r.Type() != redo.RecordTypePageWrite {
			continue
		}
		pid := r.PageId()
		if seen[pid] {
			continue
		}
		seen[pid] = true
		if _, err := bp.Page(pid); err != nil {
			continue
		}
		ids = append(ids, pid)
	}
	return ids
}

// fillPoolWithDummyPages はプールの残り空きスロットを未使用ページ番号の AddPage で埋め、pin を保持する
//   - 呼び出し側は返された PageId 群を unpinDummyPages で解放する
func fillPoolWithDummyPages(t *testing.T, bp *buffer.Pool) []page.Id {
	t.Helper()
	dummyFileId := page.FileId(99)
	dummyPath := filepath.Join(t.TempDir(), "dummy.db")
	hf, err := file.NewHeapFile(dummyPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(dummyFileId, hf)
	var ids []page.Id
	for pn := page.PageNumber(0); ; pn++ {
		pageId := page.NewId(dummyFileId, pn)
		if _, err := bp.AddPage(pageId); err != nil {
			require.ErrorIs(t, err, buffer.ErrAllPagesUnevictable)
			return ids
		}
		if _, err := bp.Page(pageId); err != nil {
			require.NoError(t, err)
		}
		ids = append(ids, pageId)
	}
}

// unpinDummyPages は fillPoolWithDummyPages で保持した pin を解放する
func unpinDummyPages(bp *buffer.Pool, ids []page.Id) {
	for _, id := range ids {
		bp.Unpin(id)
	}
}

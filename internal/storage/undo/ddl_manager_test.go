package undo

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDDLManager(t *testing.T) {
	t.Run("空の DDL Undo 領域を開ける", func(t *testing.T) {
		// GIVEN
		bp, rootPageId, _ := setupDDLTestEnv(t)

		// WHEN
		mgr, err := NewDDLManager(bp, page.FileId(0), rootPageId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, mgr)
		assert.Equal(t, rootPageId, mgr.currentPageId)
	})

	t.Run("rootPageId に無効値を渡すと ErrInvalidDDLUndoRoot を返す", func(t *testing.T) {
		// GIVEN
		bp, _, _ := setupDDLTestEnv(t)

		// WHEN
		mgr, err := NewDDLManager(bp, page.FileId(0), page.InvalidId())

		// THEN
		assert.Nil(t, mgr)
		assert.ErrorIs(t, err, ErrInvalidDDLUndoRoot)
	})

	t.Run("複数ページに渡る DDL Undo 領域から末尾ページを特定できる", func(t *testing.T) {
		// GIVEN
		bp, rootPageId, redoLog := setupDDLTestEnv(t)
		mgr, err := NewDDLManager(bp, page.FileId(0), rootPageId)
		assert.NoError(t, err)
		fillUntilPageSwitch(t, mgr, redoLog)
		tailPageId := mgr.currentPageId
		assert.NotEqual(t, rootPageId, tailPageId)

		// WHEN
		opened, err := NewDDLManager(bp, page.FileId(0), rootPageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, tailPageId, opened.currentPageId)
	})
}

func TestDDLManagerAppend(t *testing.T) {
	t.Run("1 件 Append すると ReverseScan で取得できる", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		record := NewDDLRecord(DDLRecordTypeCreateBTree, []byte{0x01, 0x02, 0x03})

		// WHEN
		appendDDLRecord(t, mgr, redoLog, record)
		records := reverseScanDDL(t, mgr)

		// THEN
		assert.Len(t, records, 1)
		assert.Equal(t, record.recordType, records[0].recordType)
		assert.Equal(t, record.payload, records[0].payload)
	})

	t.Run("複数 Append すると ReverseScan で逆順に取得できる", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		r1 := NewDDLRecord(DDLRecordTypeCreateBTree, []byte("first"))
		r2 := NewDDLRecord(DDLRecordTypeMetaInsert, []byte("second"))
		r3 := NewDDLRecord(DDLRecordTypeAllocateFileId, []byte("third"))

		// WHEN
		appendDDLRecord(t, mgr, redoLog, r1)
		appendDDLRecord(t, mgr, redoLog, r2)
		appendDDLRecord(t, mgr, redoLog, r3)
		records := reverseScanDDL(t, mgr)

		// THEN
		assert.Len(t, records, 3)
		assert.Equal(t, []byte("third"), records[0].payload)
		assert.Equal(t, []byte("second"), records[1].payload)
		assert.Equal(t, []byte("first"), records[2].payload)
	})

	t.Run("ページ満杯時に新ページを確保して next リンクを繋ぐ", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		oldPageId := mgr.currentPageId

		// WHEN
		fillUntilPageSwitch(t, mgr, redoLog)

		// THEN
		assert.NotEqual(t, oldPageId, mgr.currentPageId)
		bufPage, err := mgr.bufferPool.Page(oldPageId)
		assert.NoError(t, err)
		defer mgr.bufferPool.Unpin(oldPageId)
		oldDDLPage := NewFirstPage(bufPage)
		assert.Equal(t, mgr.currentPageId.PageNumber(), oldDDLPage.NextPageNumber())
	})

	t.Run("ページ満杯時の switch では新ページ実体化の REDO がリンク変更 REDO より前に出る", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		oldPageId := mgr.currentPageId

		// WHEN
		fillUntilPageSwitch(t, mgr, redoLog)
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

		// THEN: 新ページの実体化 REDO が旧ページのリンク変更 REDO より前に出る
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

	t.Run("上限超過レコードの Append は書き込み前に ErrRecordTooLarge を返し redo に何も記録しない", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		require.NoError(t, redoLog.Flush())
		sizeBefore, err := redoLog.Size()
		require.NoError(t, err)
		hugeRec := NewDDLRecord(DDLRecordTypeMetaInsert, make([]byte, page.Size))

		// WHEN
		mtr := buffer.NewWriteMtr(mgr.bufferPool, lock.DDLReservedTrxId, redoLog)
		appendErr := mgr.Append(mtr, hugeRec)
		mtr.UnpinAll()

		// THEN
		assert.ErrorIs(t, appendErr, ErrRecordTooLarge)
		require.NoError(t, redoLog.Flush())
		sizeAfter, err := redoLog.Size()
		require.NoError(t, err)
		assert.Equal(t, sizeBefore, sizeAfter)
	})
}

func TestDDLManagerReverseScan(t *testing.T) {
	t.Run("レコードが無い場合空の slice を返す", func(t *testing.T) {
		// GIVEN
		mgr, _ := setupDDLManager(t)

		// WHEN
		records := reverseScanDDL(t, mgr)

		// THEN
		assert.Empty(t, records)
	})

	t.Run("複数ページにまたがる場合も全件を逆順に取得できる", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		var written []DDLRecord
		oldPageId := mgr.currentPageId
		for mgr.currentPageId == oldPageId {
			r := NewDDLRecord(DDLRecordTypeMetaInsert, make([]byte, 1000))
			binary.BigEndian.PutUint32(r.payload[0:4], uint32(len(written)))
			appendDDLRecord(t, mgr, redoLog, r)
			written = append(written, r)
		}

		// WHEN
		records := reverseScanDDL(t, mgr)

		// THEN
		assert.Len(t, records, len(written))
		for i, rec := range records {
			expected := written[len(written)-1-i]
			assert.Equal(t, expected.payload, rec.payload)
		}
	})
}

func TestDDLManagerClear(t *testing.T) {
	t.Run("単一ページの専用領域を Clear すると root が残り中身が空になる", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		rootPageId := mgr.rootPageId
		appendDDLRecord(t, mgr, redoLog, NewDDLRecord(DDLRecordTypeCreateBTree, []byte{0x01}))

		// WHEN
		clearDDL(t, mgr, redoLog)

		// THEN
		assert.Equal(t, rootPageId, mgr.rootPageId)
		assert.Equal(t, rootPageId, mgr.currentPageId)
		bufPage, err := mgr.bufferPool.Page(rootPageId)
		assert.NoError(t, err)
		defer mgr.bufferPool.Unpin(rootPageId)
		rootDDLPage := NewFirstPage(bufPage)
		assert.Equal(t, uint16(0), rootDDLPage.UsedBytes())
		assert.Equal(t, page.PageNumber(0), rootDDLPage.NextPageNumber())
	})

	t.Run("複数ページの専用領域を Clear すると中間ページが解放され root は残る", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		rootPageId := mgr.rootPageId
		fillUntilPageSwitch(t, mgr, redoLog)
		tailPageId := mgr.currentPageId
		assert.NotEqual(t, rootPageId, tailPageId)

		// WHEN
		clearDDL(t, mgr, redoLog)

		// THEN
		assert.Equal(t, rootPageId, mgr.rootPageId)
		assert.Equal(t, rootPageId, mgr.currentPageId)
		readMtr := buffer.NewMtr(mgr.bufferPool)
		defer readMtr.UnpinAll()
		isFree, err := fsp.IsPageFree(readMtr, tailPageId)
		assert.NoError(t, err)
		assert.True(t, isFree)
	})

	t.Run("Clear 後に内部状態が root にリセットされる", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		rootPageId := mgr.rootPageId

		// WHEN
		clearDDL(t, mgr, redoLog)

		// THEN
		assert.Equal(t, rootPageId, mgr.rootPageId)
		assert.Equal(t, rootPageId, mgr.currentPageId)
	})

	t.Run("Clear 後も同じ DDLManager で Append 可能", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		appendDDLRecord(t, mgr, redoLog, NewDDLRecord(DDLRecordTypeCreateBTree, []byte("first")))
		clearDDL(t, mgr, redoLog)

		// WHEN
		appendDDLRecord(t, mgr, redoLog, NewDDLRecord(DDLRecordTypeMetaInsert, []byte("after-clear")))
		records := reverseScanDDL(t, mgr)

		// THEN
		assert.Len(t, records, 1)
		assert.Equal(t, []byte("after-clear"), records[0].payload)
	})

	t.Run("複数ページ Append → Clear → 再度ページ切替まで Append できる", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		rootPageId := mgr.rootPageId
		fillUntilPageSwitch(t, mgr, redoLog)
		assert.NotEqual(t, rootPageId, mgr.currentPageId)

		// WHEN
		clearDDL(t, mgr, redoLog)
		assert.Equal(t, rootPageId, mgr.currentPageId)
		fillUntilPageSwitch(t, mgr, redoLog)

		// THEN
		assert.NotEqual(t, rootPageId, mgr.currentPageId)
		records := reverseScanDDL(t, mgr)
		assert.NotEmpty(t, records)
	})

	t.Run("複数ページの Clear は 1 ページ解放ごとに独立した mini-transaction として commit される", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		fillUntilPageSwitch(t, mgr, redoLog)
		require.NoError(t, redoLog.Flush())
		baseLsn := redoLog.FlushedLsn()

		// WHEN
		clearDDL(t, mgr, redoLog)

		// THEN: root リセット (1 mtr) + 中間ページ解放 (N mtr) = 2 以上の MtrEnd が記録される
		require.NoError(t, redoLog.Flush())
		records, err := redoLog.ReadFrom(baseLsn)
		require.NoError(t, err)
		var mtrEndCount int
		for _, r := range records {
			if r.Type() == redo.RecordTypeMtrEnd {
				mtrEndCount++
			}
		}
		assert.GreaterOrEqual(t, mtrEndCount, 2)
	})

	t.Run("redoLog = nil の Clear は Redo に何も記録せずコンテナだけを空にする", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		rootPageId := mgr.rootPageId
		fillUntilPageSwitch(t, mgr, redoLog)
		tailPageId := mgr.currentPageId
		require.NotEqual(t, rootPageId, tailPageId)
		require.NoError(t, redoLog.Flush())
		sizeBefore, err := redoLog.Size()
		require.NoError(t, err)
		flushedBefore := redoLog.FlushedLsn()

		// WHEN
		require.NoError(t, mgr.Clear(lock.DDLReservedTrxId, nil))

		// THEN
		require.NoError(t, redoLog.Flush())
		sizeAfter, err := redoLog.Size()
		require.NoError(t, err)
		assert.Equal(t, sizeBefore, sizeAfter)
		assert.Equal(t, flushedBefore, redoLog.FlushedLsn())
		assert.Equal(t, rootPageId, mgr.currentPageId)
		bufPage, err := mgr.bufferPool.Page(rootPageId)
		require.NoError(t, err)
		defer mgr.bufferPool.Unpin(rootPageId)
		rootDDLPage := NewFirstPage(bufPage)
		assert.Equal(t, uint16(0), rootDDLPage.UsedBytes())
		assert.Equal(t, page.PageNumber(0), rootDDLPage.NextPageNumber())
	})
}

// setupDDLTestEnv は DDLManager テスト用に buffer.Pool / DDL Undo root を作る
func setupDDLTestEnv(t *testing.T) (*buffer.Pool, page.Id, *redo.Buffer) {
	t.Helper()
	redoLog, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Close() })

	fileId := page.FileId(0)
	path := filepath.Join(t.TempDir(), "ddl_undo_test.db")
	hf, err := file.NewHeapFile(path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })

	bp := buffer.NewPool(page.Size*20, redoLog, nil)
	bp.RegisterHeapFile(fileId, hf)

	initMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	if err := fsp.InitHeader(initMtr, fileId); err != nil {
		t.Fatalf("fsp.InitHeader に失敗: %v", err)
	}
	if err := initMtr.Commit(); err != nil {
		t.Fatalf("InitHeader Mtr の Commit に失敗: %v", err)
	}

	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	rootPageId := CreateChainRoot(mtr, fileId)
	if err := mtr.Commit(); err != nil {
		t.Fatalf("セットアップ Mtr の Commit に失敗: %v", err)
	}

	return bp, rootPageId, redoLog
}

// setupDDLManager は初期化済みの DDLManager と redo.Buffer を作る
func setupDDLManager(t *testing.T) (*DDLManager, *redo.Buffer) {
	t.Helper()
	bp, rootPageId, redoLog := setupDDLTestEnv(t)
	mgr, err := NewDDLManager(bp, page.FileId(0), rootPageId)
	if err != nil {
		t.Fatalf("DDLManager の作成に失敗: %v", err)
	}
	return mgr, redoLog
}

// appendDDLRecord は 1 件の Append を書き込み Mtr + Commit で実行するヘルパー
func appendDDLRecord(t *testing.T, mgr *DDLManager, redoLog *redo.Buffer, record DDLRecord) {
	t.Helper()
	mtr := buffer.NewWriteMtr(mgr.bufferPool, lock.DDLReservedTrxId, redoLog)
	defer mtr.UnpinAll()
	if err := mgr.Append(mtr, record); err != nil {
		t.Fatalf("Append に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
	}
}

// reverseScanDDL は ReverseScan を書き込み Mtr スコープで実行するヘルパー
func reverseScanDDL(t *testing.T, mgr *DDLManager) []DDLRecord {
	t.Helper()
	mtr := buffer.NewMtr(mgr.bufferPool)
	defer mtr.UnpinAll()
	records, err := mgr.ReverseScan(mtr)
	if err != nil {
		t.Fatalf("ReverseScan に失敗: %v", err)
	}
	return records
}

// clearDDL は Clear を実行するヘルパー
//   - Clear は内部で「1 ページ解放 = 1 mtr」の write mtr を生成・commit する
func clearDDL(t *testing.T, mgr *DDLManager, redoLog *redo.Buffer) {
	t.Helper()
	if err := mgr.Clear(lock.DDLReservedTrxId, redoLog); err != nil {
		t.Fatalf("Clear に失敗: %v", err)
	}
}

// fillUntilPageSwitch は専用ページ列が新ページに切り替わるまでレコードを書き続けるヘルパー
func fillUntilPageSwitch(t *testing.T, mgr *DDLManager, redoLog *redo.Buffer) {
	t.Helper()
	oldPageId := mgr.currentPageId
	bigPayload := make([]byte, 1000)
	for mgr.currentPageId == oldPageId {
		appendDDLRecord(t, mgr, redoLog, NewDDLRecord(DDLRecordTypeMetaInsert, bigPayload))
	}
}

package undo

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestNewDDLManager(t *testing.T) {
	t.Run("空の DDL Undo 領域を開ける", func(t *testing.T) {
		// GIVEN
		bp, freeListMapPageId, rootPageId, redoLog := setupDDLTestEnv(t)

		// WHEN
		openMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		mgr, err := NewDDLManager(openMtr, page.FileId(0), rootPageId, freeListMapPageId)
		_ = openMtr.Commit()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, mgr)
		assert.Equal(t, rootPageId, mgr.currentPageId)
	})

	t.Run("rootPageId に無効値を渡すと ErrInvalidDDLUndoRoot を返す", func(t *testing.T) {
		// GIVEN
		bp, freeListMapPageId, _, redoLog := setupDDLTestEnv(t)

		// WHEN
		openMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		mgr, err := NewDDLManager(openMtr, page.FileId(0), page.InvalidId(), freeListMapPageId)
		_ = openMtr.Commit()

		// THEN
		assert.Nil(t, mgr)
		assert.ErrorIs(t, err, ErrInvalidDDLUndoRoot)
	})

	t.Run("複数ページに渡る DDL Undo 領域から末尾ページを特定できる", func(t *testing.T) {
		// GIVEN
		bp, freeListMapPageId, rootPageId, redoLog := setupDDLTestEnv(t)
		openMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		mgr, err := NewDDLManager(openMtr, page.FileId(0), rootPageId, freeListMapPageId)
		_ = openMtr.Commit()
		assert.NoError(t, err)
		fillUntilPageSwitch(t, mgr, redoLog)
		tailPageId := mgr.currentPageId
		assert.NotEqual(t, rootPageId, tailPageId)

		// WHEN
		openMtr2 := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
		opened, err := NewDDLManager(openMtr2, page.FileId(0), rootPageId, freeListMapPageId)
		_ = openMtr2.Commit()

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
		oldDDLPage := NewPage(bufPage)
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

		// THEN
		assert.NotEqual(t, oldPageId, newPageId)
		n := len(pageWrites)
		assert.GreaterOrEqual(t, n, 2)
		assert.Equal(t, newPageId, pageWrites[n-2].PageId())
		assert.Equal(t, oldPageId, pageWrites[n-1].PageId())
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
		rootDDLPage := NewPage(bufPage)
		assert.Equal(t, uint16(0), rootDDLPage.UsedBytes())
		assert.Equal(t, page.PageNumber(0), rootDDLPage.NextPageNumber())
	})

	t.Run("複数ページの専用領域を Clear すると中間ページのみフリーリストに積まれ root は残る", func(t *testing.T) {
		// GIVEN
		mgr, redoLog := setupDDLManager(t)
		rootPageId := mgr.rootPageId
		freeListMapPageId := mgr.freeListMapPageId
		fillUntilPageSwitch(t, mgr, redoLog)
		tailPageId := mgr.currentPageId
		assert.NotEqual(t, rootPageId, tailPageId)

		// WHEN
		clearDDL(t, mgr, redoLog)

		// THEN
		assert.Equal(t, rootPageId, mgr.rootPageId)
		assert.Equal(t, rootPageId, mgr.currentPageId)
		head := readFreeListHead(t, mgr.bufferPool, freeListMapPageId, page.FileId(0))
		assert.Equal(t, tailPageId.PageNumber(), head)
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
}

// setupDDLTestEnv は DDLManager テスト用に buffer.Pool / freeListMap / DDL Undo root を作る
func setupDDLTestEnv(t *testing.T) (*buffer.Pool, page.Id, page.Id, *redo.Buffer) {
	t.Helper()
	redoLog, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Close() })

	fileId := page.FileId(0)
	path := filepath.Join(t.TempDir(), "ddl_undo_test.db")
	hf, err := file.NewHeapFile(fileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })

	bp := buffer.NewPool(page.Size*20, redoLog, nil)
	bp.RegisterHeapFile(fileId, hf)

	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	freeListMapPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		t.Fatalf("freeListMap ページの確保に失敗: %v", err)
	}
	if _, err := bp.AddPage(freeListMapPageId); err != nil {
		t.Fatalf("freeListMap ページの追加に失敗: %v", err)
	}
	freeListMapBufPage, err := mtr.PageForWrite(freeListMapPageId)
	if err != nil {
		t.Fatalf("freeListMap ページの書き込み準備に失敗: %v", err)
	}
	buffer.InitializeFreeListMapPage(freeListMapBufPage)

	rootPageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		t.Fatalf("DDL Undo root ページの確保に失敗: %v", err)
	}
	if _, err := bp.AddPage(rootPageId); err != nil {
		t.Fatalf("DDL Undo root ページの追加に失敗: %v", err)
	}
	rootBufPage, err := mtr.PageForWrite(rootPageId)
	if err != nil {
		t.Fatalf("DDL Undo root ページの書き込み準備に失敗: %v", err)
	}
	CreatePage(rootBufPage)
	if err := mtr.Commit(); err != nil {
		t.Fatalf("セットアップ Mtr の Commit に失敗: %v", err)
	}

	return bp, freeListMapPageId, rootPageId, redoLog
}

// setupDDLManager は初期化済みの DDLManager と redo.Buffer を作る
func setupDDLManager(t *testing.T) (*DDLManager, *redo.Buffer) {
	t.Helper()
	bp, freeListMapPageId, rootPageId, redoLog := setupDDLTestEnv(t)
	openMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)
	mgr, err := NewDDLManager(openMtr, page.FileId(0), rootPageId, freeListMapPageId)
	if err != nil {
		openMtr.UnpinAll()
		t.Fatalf("DDLManager の作成に失敗: %v", err)
	}
	if err := openMtr.Commit(); err != nil {
		t.Fatalf("DDLManager Commit に失敗: %v", err)
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

// clearDDL は Clear を書き込み Mtr + Commit で実行するヘルパー
func clearDDL(t *testing.T, mgr *DDLManager, redoLog *redo.Buffer) {
	t.Helper()
	mtr := buffer.NewWriteMtr(mgr.bufferPool, lock.DDLReservedTrxId, redoLog)
	defer mtr.UnpinAll()
	if err := mgr.Clear(mtr); err != nil {
		t.Fatalf("Clear に失敗: %v", err)
	}
	if err := mtr.Commit(); err != nil {
		t.Fatalf("Commit に失敗: %v", err)
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

// readFreeListHead は freeListMap ページから指定 FileId のフリーリスト先頭 PageNumber を読み取る
func readFreeListHead(t *testing.T, bp *buffer.Pool, freeListMapPageId page.Id, fileId page.FileId) page.PageNumber {
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

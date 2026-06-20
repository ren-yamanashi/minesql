package buffer

import (
	"encoding/binary"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestMtrCommitLogsModifiedPage(t *testing.T) {
	t.Run("変更ページを MtrStart/PageWrite/MtrEnd で囲んで記録する", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId := addPage(t, bp, 0, 0)
		mtr := NewWriteMtr(bp, lock.TrxId(1), rl)
		bufPage, err := mtr.PageForWrite(pageId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{1, 2, 3})

		// WHEN
		assert.NoError(t, mtr.Commit())

		// THEN
		records := readRedoRecords(t, rl)
		assert.Equal(t, []redo.RecordType{
			redo.RecordTypeMtrStart, redo.RecordTypePageWrite, redo.RecordTypeMtrEnd,
		}, recordTypes(records))
		assert.Equal(t, pageId, records[1].PageId())
	})
}

func TestMtrCommitSkipsUnmodifiedPage(t *testing.T) {
	t.Run("PageForWrite しても未変更なら境界マーカーも含め何も記録しない", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId := addPage(t, bp, 0, 0)
		mtr := NewWriteMtr(bp, lock.TrxId(1), rl)
		_, err := mtr.PageForWrite(pageId)
		assert.NoError(t, err)

		// WHEN
		assert.NoError(t, mtr.Commit())

		// THEN
		assert.Empty(t, readRedoRecords(t, rl))
	})
}

func TestMtrCommitSkipsReadOnlyPage(t *testing.T) {
	t.Run("書き込み Mtr でも S 取得のみのページは記録しない", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId := addPage(t, bp, 0, 0)
		mtr := NewWriteMtr(bp, lock.TrxId(1), rl)
		_, err := mtr.PageForRead(pageId)
		assert.NoError(t, err)

		// WHEN
		assert.NoError(t, mtr.Commit())

		// THEN
		assert.Empty(t, readRedoRecords(t, rl))
	})
}

func TestMtrReadMtrLogsNothing(t *testing.T) {
	t.Run("読み取り専用 Mtr (NewMtr) は変更しても Redo を記録しない", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId := addPage(t, bp, 0, 0)
		mtr := NewMtr(bp)
		bufPage, err := mtr.PageForWrite(pageId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{1})

		// WHEN
		mtr.UnpinAll()

		// THEN
		assert.Empty(t, readRedoRecords(t, rl))
	})
}

func TestMtrCommitStampsPageLsn(t *testing.T) {
	t.Run("Page LSN がスタンプされ PageWrite レコードの LSN とコピー内ヘッダーが一致する", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId := addPage(t, bp, 0, 0)
		mtr := NewWriteMtr(bp, lock.TrxId(1), rl)
		bufPage, err := mtr.PageForWrite(pageId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{7})

		// WHEN
		assert.NoError(t, mtr.Commit())

		// THEN
		records := readRedoRecords(t, rl)
		pageWrite := records[1]
		recordLsn := pageWrite.Lsn()
		liveLsn := redo.Lsn(binary.BigEndian.Uint32(bufPage.data.Header()))
		copyLsn := redo.Lsn(binary.BigEndian.Uint32(pageWrite.Data().Header()))
		assert.NotEqual(t, redo.Lsn(0), liveLsn)
		assert.Equal(t, recordLsn, liveLsn)
		assert.Equal(t, recordLsn, copyLsn)
	})
}

func TestMtrUnpinLogsBeforeRelease(t *testing.T) {
	t.Run("個別 Unpin の時点で変更ページが記録され Commit で二重記録されない", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId := addPage(t, bp, 0, 0)
		mtr := NewWriteMtr(bp, lock.TrxId(1), rl)
		bufPage, err := mtr.PageForWrite(pageId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{9})

		// WHEN: Commit 前の Unpin で記録される
		mtr.Unpin(pageId)

		// THEN: MtrEnd 前だが PageWrite は既に記録済み
		afterUnpin := readRedoRecords(t, rl)
		assert.Equal(t, []redo.RecordType{
			redo.RecordTypeMtrStart, redo.RecordTypePageWrite,
		}, recordTypes(afterUnpin))

		// WHEN: Commit で MtrEnd のみ追加され、二重記録しない
		assert.NoError(t, mtr.Commit())

		// THEN
		all := readRedoRecords(t, rl)
		assert.Equal(t, []redo.RecordType{
			redo.RecordTypeMtrStart, redo.RecordTypePageWrite, redo.RecordTypeMtrEnd,
		}, recordTypes(all))
	})
}

func TestMtrCommitStampsOldestModificationFromMtrStart(t *testing.T) {
	t.Run("初回 Commit で oldestModificationLsn が MtrStart の LSN になる", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId := addPage(t, bp, 0, 0)
		mtr := NewWriteMtr(bp, lock.TrxId(1), rl)
		bufPage, err := mtr.PageForWrite(pageId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{1})

		// WHEN
		assert.NoError(t, mtr.Commit())

		// THEN
		records := readRedoRecords(t, rl)
		var startLsn redo.Lsn
		for _, r := range records {
			if r.Type() == redo.RecordTypeMtrStart {
				startLsn = r.Lsn()
				break
			}
		}
		assert.NotEqual(t, redo.Lsn(0), startLsn)
		assert.Equal(t, startLsn, bufPage.oldestModificationLsn)
	})

	t.Run("同じページを別 Mtr で再変更しても oldestModificationLsn は最初の MtrStart LSN を保持する", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId := addPage(t, bp, 0, 0)
		mtr1 := NewWriteMtr(bp, lock.TrxId(1), rl)
		bufPage, err := mtr1.PageForWrite(pageId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{1})
		assert.NoError(t, mtr1.Commit())
		firstOldest := bufPage.oldestModificationLsn

		// WHEN
		mtr2 := NewWriteMtr(bp, lock.TrxId(1), rl)
		bufPage2, err := mtr2.PageForWrite(pageId)
		assert.NoError(t, err)
		bufPage2.WriteBodyAt(1, []byte{2})
		assert.NoError(t, mtr2.Commit())

		// THEN
		assert.NotEqual(t, redo.Lsn(0), firstOldest)
		assert.Equal(t, firstOldest, bufPage.oldestModificationLsn)
	})

	t.Run("同じ Mtr 内の複数ページは同じ MtrStart LSN を共有する", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId1 := addPage(t, bp, 0, 0)
		pageId2 := addPage(t, bp, 0, 1)
		mtr := NewWriteMtr(bp, lock.TrxId(1), rl)
		p1, err := mtr.PageForWrite(pageId1)
		assert.NoError(t, err)
		p1.WriteBodyAt(0, []byte{1})
		p2, err := mtr.PageForWrite(pageId2)
		assert.NoError(t, err)
		p2.WriteBodyAt(0, []byte{2})

		// WHEN
		assert.NoError(t, mtr.Commit())

		// THEN
		records := readRedoRecords(t, rl)
		var startLsn redo.Lsn
		for _, r := range records {
			if r.Type() == redo.RecordTypeMtrStart {
				startLsn = r.Lsn()
				break
			}
		}
		assert.NotEqual(t, redo.Lsn(0), startLsn)
		assert.Equal(t, startLsn, p1.oldestModificationLsn)
		assert.Equal(t, startLsn, p2.oldestModificationLsn)
	})

	t.Run("フラッシュで oldestModificationLsn が 0 にリセットされ、次の Mtr で新しい LSN が設定される", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)
		pageId := addPage(t, bp, 0, 0)
		mtr1 := NewWriteMtr(bp, lock.TrxId(1), rl)
		bufPage, err := mtr1.PageForWrite(pageId)
		assert.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{1})
		assert.NoError(t, mtr1.Commit())
		firstOldest := bufPage.oldestModificationLsn
		assert.NoError(t, bp.FlushAllPages())

		// WHEN
		mtr2 := NewWriteMtr(bp, lock.TrxId(1), rl)
		bufPage2, err := mtr2.PageForWrite(pageId)
		assert.NoError(t, err)
		bufPage2.WriteBodyAt(1, []byte{2})
		assert.NoError(t, mtr2.Commit())

		// THEN
		assert.NotEqual(t, redo.Lsn(0), bufPage.oldestModificationLsn)
		assert.NotEqual(t, firstOldest, bufPage.oldestModificationLsn)
	})
}

func TestMtrCommitLogsInLifoOrder(t *testing.T) {
	t.Run("保持中の変更ページは LIFO 順 (後に取得したページが先) で記録される", func(t *testing.T) {
		// GIVEN
		rl, bp := newRedoAndPool(t)
		pageId1 := addPage(t, bp, 0, 0)
		pageId2 := addPage(t, bp, 0, 1)
		mtr := NewWriteMtr(bp, lock.TrxId(1), rl)
		p1, err := mtr.PageForWrite(pageId1)
		assert.NoError(t, err)
		p1.WriteBodyAt(0, []byte{1})
		p2, err := mtr.PageForWrite(pageId2)
		assert.NoError(t, err)
		p2.WriteBodyAt(0, []byte{2})

		// WHEN
		assert.NoError(t, mtr.Commit())

		// THEN
		var pageOrder []page.Id
		for _, r := range readRedoRecords(t, rl) {
			if r.Type() == redo.RecordTypePageWrite {
				pageOrder = append(pageOrder, r.PageId())
			}
		}
		assert.Equal(t, []page.Id{pageId2, pageId1}, pageOrder)
	})
}

// newRedoAndPool はテスト用の実 Redo バッファとバッファプールを生成する
func newRedoAndPool(t *testing.T) (*redo.Buffer, *Pool) {
	t.Helper()
	rl, err := redo.NewBuffer(t.TempDir())
	assert.NoError(t, err)
	t.Cleanup(func() { _ = rl.Close() })
	return rl, NewPool(page.Size*4, rl, nil)
}

// newTestRedoLog はテスト用の redo.Buffer を生成する
func newTestRedoLog(t *testing.T) *redo.Buffer {
	t.Helper()
	rl, err := redo.NewBuffer(t.TempDir())
	assert.NoError(t, err)
	t.Cleanup(func() { _ = rl.Close() })
	return rl
}

// addPage はバッファプールに新しいページを追加し、その PageId を返す
func addPage(t *testing.T, bp *Pool, fileId page.FileId, num page.PageNumber) page.Id {
	t.Helper()
	pageId := page.NewId(fileId, num)
	_, err := bp.AddPage(pageId)
	assert.NoError(t, err)
	return pageId
}

// readRedoRecords は Redo バッファをフラッシュして全レコードを読み出す
func readRedoRecords(t *testing.T, rl *redo.Buffer) []redo.Record {
	t.Helper()
	assert.NoError(t, rl.Flush())
	records, err := rl.ReadFrom(redo.Lsn(0))
	assert.NoError(t, err)
	return records
}

// recordTypes はレコード列のレコード種別だけを取り出す
func recordTypes(records []redo.Record) []redo.RecordType {
	types := make([]redo.RecordType, len(records))
	for i, r := range records {
		types[i] = r.Type()
	}
	return types
}

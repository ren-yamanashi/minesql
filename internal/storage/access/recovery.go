package access

import (
	"encoding/binary"
	"errors"
	"io"
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type Recovery struct {
	redoLog     *redo.Buffer
	bufferPool  *buffer.Pool
	transaction *TrxManager
	undoFileId  page.FileId // Undo ログの FileId
	ddlManager  *undo.DDLManager
}

func NewRecovery(
	redo *redo.Buffer,
	bp *buffer.Pool,
	trx *TrxManager,
	undoFileId page.FileId,
	ddlManager *undo.DDLManager,
) *Recovery {
	return &Recovery{
		redoLog:     redo,
		bufferPool:  bp,
		transaction: trx,
		undoFileId:  undoFileId,
		ddlManager:  ddlManager,
	}
}

// NeedsRecovery はリカバリが必要であるかを判定する
// - チェックポイント LSN 以降に Redo ログが残っている (=前回異常終了した) 場合はリカバリが必要
func (r *Recovery) NeedsRecovery() (bool, error) {
	records, err := r.redoLog.ReadFrom(r.redoLog.CheckpointLsn())
	if err != nil {
		return false, err
	}
	return len(records) > 0, nil
}

// Execute はリカバリを実行する
func (r *Recovery) Execute() error {
	records, err := r.redoLog.ReadFrom(r.redoLog.CheckpointLsn())
	if err != nil {
		return err
	}

	if err := r.applyRedoLog(records); err != nil {
		return err
	}
	if err := r.applyRollback(records); err != nil {
		return err
	}
	if err := r.applyDDLRollback(records); err != nil {
		return err
	}
	if err := r.transaction.PersistNextTrxIdToCatalog(); err != nil {
		return err
	}
	if err := r.bufferPool.FlushAllPages(); err != nil {
		return err
	}
	return NewCheckpoint(r.bufferPool, r.redoLog, r.transaction).Execute()
}

// applyRedoLog は Redo ログを先頭からスキャンし、完全な mini-transaction のページ変更を適用する
func (r *Recovery) applyRedoLog(records []redo.Record) error {
	pendingByTrx := make(map[lock.TrxId][]redo.Record)

	for _, rec := range records {
		switch rec.Type() {
		case redo.RecordTypeMtrStart:
			pendingByTrx[rec.TrxId()] = nil
		case redo.RecordTypeMtrEnd:
			for _, pending := range pendingByTrx[rec.TrxId()] {
				if err := r.applyPageWrite(pending); err != nil {
					return err
				}
			}
			delete(pendingByTrx, rec.TrxId())
		case redo.RecordTypePageWrite:
			if pending, ok := pendingByTrx[rec.TrxId()]; ok {
				pendingByTrx[rec.TrxId()] = append(pending, rec)
			}
		}
	}
	return nil
}

// applyPageWrite は 1 件のページ変更レコードを適用する
//   - 対象ページが disk に存在しない場合は buffer pool に空ページを確保してから上書きする
func (r *Recovery) applyPageWrite(rec redo.Record) error {
	mtr := buffer.NewMtr(r.bufferPool)
	defer mtr.UnpinAll()

	writePage, err := mtr.PageForWrite(rec.PageId())
	if errors.Is(err, io.EOF) {
		if _, addErr := r.bufferPool.AddPage(rec.PageId()); addErr != nil {
			return addErr
		}
		writePage, err = mtr.PageForWrite(rec.PageId())
	}
	if err != nil {
		return err
	}
	currentLsn := redo.Lsn(binary.BigEndian.Uint32(writePage.Data().Header()))
	if currentLsn >= rec.Lsn() {
		return nil
	}
	writePage.OverwritePage(rec.Data().Bytes())
	return nil
}

// applyRollback は Redo ログから未完了トランザクションを特定し、Undo ログを走査してロールバックする
func (r *Recovery) applyRollback(records []redo.Record) error {
	completed := make(map[lock.TrxId]bool)
	active := make(map[lock.TrxId]bool)
	for _, rec := range records {
		active[rec.TrxId()] = true
		if rec.Type() == redo.RecordTypeCommit || rec.Type() == redo.RecordTypeRollback {
			completed[rec.TrxId()] = true
		}
	}

	for trxId := range active {
		if completed[trxId] {
			continue
		}
		if err := r.rollbackTrx(trxId); err != nil {
			return err
		}
	}
	return nil
}

// rollbackTrx は指定トランザクションの Undo レコードを逆順に適用してロールバックする
//   - Recovery 中の Undo 逆適用は読み取り Mtr で行い、Commit せず UnpinAll で終わる
//     ことで Redo に MtrStart も MtrEnd も書かない (= Redo を発生させない)
func (r *Recovery) rollbackTrx(trxId lock.TrxId) error {
	undoRecords, err := r.collectUndoRecords(trxId)
	if err != nil {
		return err
	}
	// Undo レコードを逆順に適用する (最後の操作から順に取り消す)
	for _, record := range slices.Backward(undoRecords) {
		mtr := buffer.NewMtr(r.bufferPool)
		if err := r.transaction.rollbackRecord(mtr, record); err != nil {
			mtr.UnpinAll()
			return err
		}
		mtr.UnpinAll()
	}
	return nil
}

// applyDDLRollback は DDL 予約 ID で書かれた未完了 DDL を取り消す
//   - Redo に DDL 予約 ID の Commit / Rollback レコードが無い場合のみ実行する
func (r *Recovery) applyDDLRollback(records []redo.Record) error {
	isActive := false
	isCompleted := false
	for _, rec := range records {
		if rec.TrxId() != lock.DDLReservedTrxId {
			continue
		}
		isActive = true
		if rec.Type() == redo.RecordTypeCommit || rec.Type() == redo.RecordTypeRollback {
			isCompleted = true
		}
	}
	if !isActive || isCompleted {
		return nil
	}

	scanMtr := buffer.NewMtr(r.bufferPool)
	defer scanMtr.UnpinAll()
	ddlRecords, err := r.ddlManager.ReverseScan(scanMtr)
	if err != nil {
		return err
	}

	for _, record := range ddlRecords {
		for {
			mtr := buffer.NewMtr(r.bufferPool)
			done, err := r.transaction.applyDDLRollbackRecord(mtr, record)
			if err != nil {
				mtr.UnpinAll()
				return err
			}
			mtr.UnpinAll()
			if done {
				break
			}
		}
	}

	return r.ddlManager.Clear(lock.DDLReservedTrxId, nil)
}

// collectUndoRecords は Undo ページを走査して指定トランザクションのレコードを収集する
func (r *Recovery) collectUndoRecords(trxId lock.TrxId) ([]undo.Record, error) {
	pageNum := undo.ChainHeadPageNumber
	var records []undo.Record
	for {
		pageId := page.NewId(r.undoFileId, pageNum)
		nextPageNum, ok, err := r.collectFromUndoPage(pageId, trxId, &records)
		if err != nil {
			return nil, err
		}
		if !ok || nextPageNum == 0 {
			break
		}
		pageNum = nextPageNum
	}
	return records, nil
}

// collectFromUndoPage は 1 つの Undo ページから指定トランザクションのレコードを抽出し、次ページ番号を返す
//   - ok=false: ページが存在せずチェーン終端に達したことを示す
func (r *Recovery) collectFromUndoPage(
	pageId page.Id,
	trxId lock.TrxId,
	records *[]undo.Record,
) (page.PageNumber, bool, error) {
	mtr := buffer.NewMtr(r.bufferPool)
	defer mtr.UnpinAll()

	readPage, err := mtr.PageForRead(pageId)
	if err != nil {
		return 0, false, nil
	}

	undoPage := openUndoPageForCollect(readPage, pageId)
	offset := 0
	for offset < int(undoPage.UsedBytes()) {
		recordBytes := undoPage.Record(offset)
		if recordBytes == nil {
			break
		}
		fields, err := undo.DeserializeFields(recordBytes)
		if err != nil {
			return 0, false, err
		}
		offset += len(recordBytes)
		if fields.TrxId() != trxId {
			continue
		}
		record, err := fields.ToRecord()
		if err != nil {
			return 0, false, err
		}
		*records = append(*records, record)
	}
	return undoPage.NextPageNumber(), true, nil
}

// openUndoPageForCollect は走査対象ページのチェーン内位置に応じた undo.Page ビューを返す
//   - 通常 Undo ファイルではチェーン先頭ページは undo.ChainHeadPageNumber
func openUndoPageForCollect(bufPage *buffer.Page, pageId page.Id) *undo.Page {
	if pageId.PageNumber() == undo.ChainHeadPageNumber {
		return undo.NewFirstPage(bufPage)
	}
	return undo.NewPage(bufPage)
}

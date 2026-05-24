package access

import (
	"encoding/binary"
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
}

func NewRecovery(redo *redo.Buffer, bp *buffer.Pool, trx *TrxManager, undoFileId page.FileId) *Recovery {
	return &Recovery{
		redoLog:     redo,
		bufferPool:  bp,
		transaction: trx,
		undoFileId:  undoFileId,
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
	if err := r.bufferPool.FlushAllPages(); err != nil {
		return err
	}
	return r.redoLog.Clear()
}

// applyRedoLog は Redo ログを先頭からスキャンし、ページ変更レコードを順に適用する
func (r *Recovery) applyRedoLog(records []redo.Record) error {
	for _, rec := range records {
		if rec.Type() != redo.RecordTypePageWrite {
			continue
		}

		// Redo レコードの PageId から変更ページ取得
		writePage, err := r.bufferPool.PageForWrite(rec.PageId())
		if err != nil {
			return err
		}

		// Page LSN を比較し、すでに適用済みならスキップ
		currentLsn := redo.Lsn(binary.BigEndian.Uint32(writePage.Data().Header()))
		if currentLsn >= rec.Lsn() {
			continue
		}

		// ページ全体のコピーで上書き
		recData := rec.Data()
		copy(writePage.Data().Bytes(), recData.Bytes())
	}
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
func (r *Recovery) rollbackTrx(trxId lock.TrxId) error {
	undoRecords, err := r.collectUndoRecords(trxId)
	if err != nil {
		return err
	}
	// Undo レコードを逆順に適用する (最後の操作から順に取り消す)
	for _, record := range slices.Backward(undoRecords) {
		if err := r.transaction.rollbackRecord(record); err != nil {
			return err
		}
	}
	return nil
}

// collectUndoRecords は Undo ページを走査して指定トランザクションのレコードを収集する
func (r *Recovery) collectUndoRecords(trxId lock.TrxId) ([]undo.Record, error) {
	pageNum := page.PageNumber(0)
	var records []undo.Record
	for {
		pageId := page.NewId(r.undoFileId, pageNum)
		readPage, readErr := r.bufferPool.PageForRead(pageId)
		if readErr != nil {
			// Undo ページチェーンの終端に達した場合は正常終了
			// GetReadPage はページが存在しない場合もエラーを返すため、先頭ページの読み取り失敗はチェーンが空であることを意味する
			break
		}

		undoPage := undo.NewPage(*readPage.Data())
		offset := 0
		for offset < int(undoPage.UsedBytes()) {
			recordBytes := undoPage.Record(offset)
			if recordBytes == nil {
				break
			}

			fields, deserializeErr := undo.DeserializeFields(recordBytes)
			if deserializeErr != nil {
				return nil, deserializeErr
			}

			if fields.TrxId() == trxId {
				record, toRecordErr := fields.ToRecord()
				if toRecordErr != nil {
					return nil, toRecordErr
				}
				records = append(records, record)
			}
			offset += len(recordBytes)
		}

		nextPageNum := undoPage.NextPageNumber()
		if nextPageNum == 0 {
			break
		}
		pageNum = nextPageNum
	}

	return records, nil //nolint:nilerr // readErr はページ未存在を示し、チェーン終端として正常扱い
}

package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
)

// UndoLog は全トランザクションの Undo レコードをトランザクションごとに管理する
//
// UNDO レコードはバッファプール上の UNDO ページに永続化される
type UndoLog struct {
	bp            *buffer.BufferPool
	redoLog       *log.RedoLog
	undoFileId    page.FileId            // UNDO ファイルの FileId
	currentPageId page.PageId            // 現在書き込み中の UNDO ページ
	records       map[TrxId][]UndoRecord // メモリ上の UndoRecord (table 参照を含む)
}

func NewUndoLog(bp *buffer.BufferPool, redoLog *log.RedoLog, undoFileId page.FileId) (*UndoLog, error) {
	// UNDO ページを割り当て
	pageId, err := bp.AllocatePageId(undoFileId)
	if err != nil {
		return nil, err
	}
	bufPage, err := bp.AddPage(pageId)
	if err != nil {
		return nil, err
	}
	NewUndoPage(page.NewPage(bufPage.GetWriteData())).Initialize()

	return &UndoLog{
		bp:            bp,
		redoLog:       redoLog,
		undoFileId:    undoFileId,
		currentPageId: pageId,
		records:       make(map[TrxId][]UndoRecord),
	}, nil
}

// Append は指定した trxId の Undo ログにレコードを追加する
func (u *UndoLog) Append(trxId TrxId, record UndoRecord) error {
	undoNo := uint64(len(u.records[trxId]))
	if err := u.writeToPage(trxId, record.Serialize(trxId, undoNo)); err != nil {
		return err
	}
	u.records[trxId] = append(u.records[trxId], record)
	return nil
}

// GetRecords は指定した trxId の Undo ログレコードを取得する
func (u *UndoLog) GetRecords(trxId TrxId) []UndoRecord {
	records := u.records[trxId]
	if len(records) == 0 {
		return nil
	}
	return records
}

// PopLast は指定した trxId の Undo ログの最後のレコードを削除する
//
// メモリインデックスの操作のみ (UNDO ページ上のデータは残る)
func (u *UndoLog) PopLast(trxId TrxId) {
	records := u.records[trxId]
	if len(records) > 0 {
		u.records[trxId] = records[:len(records)-1]
	}
}

// Discard は指定した trxId の Undo ログを破棄する
//
// メモリインデックスの操作のみ (UNDO ページ上のデータは残る)
func (u *UndoLog) Discard(trxId TrxId) {
	delete(u.records, trxId)
}

// writeToPage はシリアライズ済みの UNDO レコードを UNDO ページに書き込む
func (u *UndoLog) writeToPage(trxId TrxId, serialized []byte) error {
	bufPage, err := u.bp.FetchPage(u.currentPageId)
	if err != nil {
		return err
	}
	undoPage := NewUndoPage(page.NewPage(bufPage.GetWriteData()))

	if !undoPage.Append(serialized) {
		// ページが満杯なので新しいページを割り当て
		newPageId, err := u.bp.AllocatePageId(u.undoFileId)
		if err != nil {
			return err
		}
		newBufPage, err := u.bp.AddPage(newPageId)
		if err != nil {
			return err
		}

		// 現在のページに次ページへのリンクを設定
		undoPage.SetNextPageNumber(uint16(newPageId.PageNumber))

		// 新しいページを初期化してレコードを追記
		newUndoPage := NewUndoPage(page.NewPage(newBufPage.GetWriteData()))
		newUndoPage.Initialize()
		newUndoPage.Append(serialized)

		u.currentPageId = newPageId
		bufPage = newBufPage
	}

	// REDO ログに UNDO ページの変更を記録
	if u.redoLog != nil {
		u.redoLog.AppendPageImage(trxId, u.currentPageId, bufPage.GetReadData())
	}
	return nil
}

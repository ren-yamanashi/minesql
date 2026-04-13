package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
)

// undoEntry は UndoRecord とその種別を保持する
type undoEntry struct {
	recordType UndoRecordType
	record     UndoRecord
}

// UndoManager は全トランザクションの Undo レコードをトランザクションごとに管理する
//
// UNDO レコードはバッファプール上の UNDO ページに永続化される
type UndoManager struct {
	bp            *buffer.BufferPool
	redoLog       *log.RedoLog
	undoFileId    page.FileId           // UNDO ファイルの FileId
	currentPageId page.PageId           // 現在書き込み中の UNDO ページ
	entries       map[TrxId][]undoEntry // メモリ上の UndoRecord (table 参照を含む)
}

func NewUndoManager(bp *buffer.BufferPool, redoLog *log.RedoLog, undoFileId page.FileId) (*UndoManager, error) {
	// UNDO ページを割り当て
	pageId, err := bp.AllocatePageId(undoFileId)
	if err != nil {
		return nil, err
	}
	err = bp.AddPage(pageId)
	if err != nil {
		return nil, err
	}
	data, err := bp.GetWritePageData(pageId)
	if err != nil {
		return nil, err
	}
	NewUndoPage(page.NewPage(data)).Initialize()

	return &UndoManager{
		bp:            bp,
		redoLog:       redoLog,
		undoFileId:    undoFileId,
		currentPageId: pageId,
		entries:       make(map[TrxId][]undoEntry),
	}, nil
}

// Append は指定した trxId の Undo ログにレコードを追加し、書き込み先の UndoPtr を返す
func (u *UndoManager) Append(trxId TrxId, recordType UndoRecordType, record UndoRecord) (UndoPtr, error) {
	undoNo := uint64(len(u.entries[trxId]))
	ptr, err := u.writeToPage(trxId, record.Serialize(trxId, undoNo))
	if err != nil {
		return UndoPtr{}, err
	}
	u.entries[trxId] = append(u.entries[trxId], undoEntry{recordType: recordType, record: record})
	return ptr, nil
}

// GetRecords は指定した trxId の Undo ログレコードを取得する
func (u *UndoManager) GetRecords(trxId TrxId) []UndoRecord {
	entries := u.entries[trxId]
	if len(entries) == 0 {
		return nil
	}
	records := make([]UndoRecord, len(entries))
	for i, e := range entries {
		records[i] = e.record
	}
	return records
}

// PopLast は指定した trxId の Undo ログの最後のレコードを削除する
//
// メモリインデックスの操作のみ (UNDO ページ上のデータは残る)
func (u *UndoManager) PopLast(trxId TrxId) {
	entries := u.entries[trxId]
	if len(entries) > 0 {
		u.entries[trxId] = entries[:len(entries)-1]
	}
}

// DiscardInsertRecords は指定した trxId の INSERT undo レコードのみ破棄する (COMMIT 用)
//
// UPDATE/DELETE の undo レコードは他トランザクションの ReadView から undo チェーン辿りに必要なため保持する
func (u *UndoManager) DiscardInsertRecords(trxId TrxId) {
	entries := u.entries[trxId]
	kept := make([]undoEntry, 0, len(entries))
	for _, e := range entries {
		if e.recordType != UndoInsert {
			kept = append(kept, e)
		}
	}
	if len(kept) == 0 {
		delete(u.entries, trxId)
	} else {
		u.entries[trxId] = kept
	}
}

// Purge はパージ閾値より古いコミット済みトランザクションの undo エントリを破棄する
func (u *UndoManager) Purge(purgeLimit TrxId, committedTrxIds []TrxId) {
	for _, trxId := range committedTrxIds {
		if trxId < purgeLimit {
			delete(u.entries, trxId)
		}
	}
}

// Discard は指定した trxId の Undo ログをすべて破棄する (ROLLBACK 用)
//
// メモリインデックスの操作のみ (UNDO ページ上のデータは残る)
func (u *UndoManager) Discard(trxId TrxId) {
	delete(u.entries, trxId)
}

// ReadAt は UndoPtr が指す位置から undo レコードのバイト列を読み取る
func (u *UndoManager) ReadAt(ptr UndoPtr) ([]byte, error) {
	pageId := page.NewPageId(u.undoFileId, page.PageNumber(ptr.PageNumber))
	data, err := u.bp.GetReadPageData(pageId)
	if err != nil {
		return nil, err
	}
	undoPage := NewUndoPage(page.NewPage(data))
	record := undoPage.RecordAt(int(ptr.Offset))
	if record == nil {
		return nil, ErrInvalidUndoRecord
	}
	return record, nil
}

// writeToPage はシリアライズ済みの UNDO レコードを UNDO ページに書き込み、書き込み先の UndoPtr を返す
func (u *UndoManager) writeToPage(trxId TrxId, serialized []byte) (UndoPtr, error) {
	data, err := u.bp.GetWritePageData(u.currentPageId)
	if err != nil {
		return UndoPtr{}, err
	}
	undoPage := NewUndoPage(page.NewPage(data))

	// 書き込み先の位置を記録
	ptr := UndoPtr{
		PageNumber: uint16(u.currentPageId.PageNumber),
		Offset:     undoPage.UsedBytes(),
	}

	if !undoPage.Append(serialized) {
		// ページが満杯なので新しいページを割り当て
		newPageId, err := u.bp.AllocatePageId(u.undoFileId)
		if err != nil {
			return UndoPtr{}, err
		}
		err = u.bp.AddPage(newPageId)
		if err != nil {
			return UndoPtr{}, err
		}

		// 現在のページに次ページへのリンクを設定
		undoPage.SetNextPageNumber(uint16(newPageId.PageNumber))

		// 新しいページを初期化してレコードを追記
		newData, err := u.bp.GetWritePageData(newPageId)
		if err != nil {
			return UndoPtr{}, err
		}
		newUndoPage := NewUndoPage(page.NewPage(newData))
		newUndoPage.Initialize()

		// 新しいページの先頭に書き込む
		ptr = UndoPtr{
			PageNumber: uint16(newPageId.PageNumber),
			Offset:     0,
		}
		newUndoPage.Append(serialized)

		u.currentPageId = newPageId
	}

	// REDO ログに UNDO ページの変更を記録
	if u.redoLog != nil {
		readData, err := u.bp.GetReadPageData(u.currentPageId)
		if err != nil {
			return UndoPtr{}, err
		}
		u.redoLog.AppendPageCopy(trxId, u.currentPageId, readData)
	}
	return ptr, nil
}

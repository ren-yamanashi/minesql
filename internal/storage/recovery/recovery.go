package recovery

import (
	"encoding/binary"
	"fmt"
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
)

// Recovery はクラッシュリカバリを実行する
type Recovery struct {
	redoLog    *log.RedoLog
	bufferPool *buffer.BufferPool
	catalog    *dictionary.Catalog
	undoFileId page.FileId
}

// NewRecovery は Recovery を生成する
func NewRecovery(redoLog *log.RedoLog, bp *buffer.BufferPool, catalog *dictionary.Catalog, undoFileId page.FileId) *Recovery {
	return &Recovery{
		redoLog:    redoLog,
		bufferPool: bp,
		catalog:    catalog,
		undoFileId: undoFileId,
	}
}

// NeedsRecovery は REDO ログにレコードが残っている (= 前回異常終了した) かを判定する
func (r *Recovery) NeedsRecovery() (bool, error) {
	records, err := r.redoLog.ReadAll()
	if err != nil {
		return false, err
	}
	return len(records) > 0, nil
}

// Run は以下の手順でリカバリを実行する
//  1. REDO 適用
//  2. UNDO ロールバック
//  3. フラッシュ
//  4. REDO クリア
func (r *Recovery) Run() error {
	records, err := r.redoLog.ReadAll()
	if err != nil {
		return err
	}

	// REDO 適用
	if err := r.redoApply(records); err != nil {
		return err
	}

	// UNDO ロールバック
	if err := r.undoRollback(records); err != nil {
		return err
	}

	// 全ダーティーページをフラッシュ
	if err := r.bufferPool.FlushAllPages(); err != nil {
		return err
	}

	// REDO ログをクリア
	return r.redoLog.Reset()
}

// redoApply は REDO ログを先頭からスキャンし、ページ変更レコードを順に適用する
func (r *Recovery) redoApply(records []log.RedoRecord) error {
	for _, rec := range records {
		if rec.Type != log.RedoPageWrite {
			continue
		}

		// REDO レコードの PageId からページを取得
		readData, err := r.bufferPool.GetReadPageData(rec.PageId)
		if err != nil {
			return err
		}

		// Page LSN 比較し、すでに適用済みならスキップ
		pg := page.NewPage(readData)
		currentLSN := log.LSN(binary.BigEndian.Uint32(pg.Header))
		if currentLSN >= rec.LSN {
			continue
		}

		// ページ全体のコピーで上書き (Page LSN もコピーに含まれている)
		data, err := r.bufferPool.GetWritePageData(rec.PageId)
		if err != nil {
			return err
		}
		copy(data, rec.Data)
	}
	return nil
}

// undoRollback は REDO ログから未完了トランザクションを特定し、UNDO ページを走査してロールバックする
func (r *Recovery) undoRollback(records []log.RedoRecord) error {
	// コミット/ロールバック済みトランザクションを特定
	completed := make(map[uint64]bool)
	active := make(map[uint64]bool)
	for _, rec := range records {
		active[rec.TrxId] = true
		if rec.Type == log.RedoCommit || rec.Type == log.RedoRollback {
			completed[rec.TrxId] = true
		}
	}

	// 未完了トランザクションをロールバック
	for trxId := range active {
		if completed[trxId] {
			continue
		}
		if err := r.rollbackTransaction(trxId); err != nil {
			return err
		}
	}
	return nil
}

// rollbackTransaction は指定トランザクションの UNDO レコードを逆順に適用してロールバックする
func (r *Recovery) rollbackTransaction(trxId uint64) error {
	// UNDO ページを走査して該当トランザクションのレコードを収集
	undoRecords := r.collectUndoRecords(trxId)

	// リカバリ用の LockManager (他のトランザクションがないのでロック競合は発生しない)
	lockMgr := lock.NewManager(5000)

	// 逆順に適用
	for i := len(undoRecords) - 1; i >= 0; i-- {
		rec := undoRecords[i]

		// カタログからテーブルを構築 (undoLog=nil, redoLog=nil でリカバリ中に新たな UNDO/REDO を記録しない)
		table, err := r.buildTable(rec.tableName)
		if err != nil {
			return fmt.Errorf("recovery: failed to build table %s: %w", rec.tableName, err)
		}

		// UndoRecord を再構築して Undo() を呼ぶ
		var undoRecord access.UndoRecord
		switch rec.recordType {
		case access.UndoInsert:
			undoRecord = access.NewUndoInsertRecord(table, rec.columns[0])
		case access.UndoDelete:
			undoRecord = access.NewUndoDeleteRecord(table, rec.columns[0])
		case access.UndoUpdateInplace:
			undoRecord = access.NewUndoUpdateInplaceRecord(table, rec.columns[0], rec.columns[1])
		default:
			return fmt.Errorf("recovery: unknown undo record type: %d", rec.recordType)
		}

		if err := undoRecord.Undo(r.bufferPool, trxId, lockMgr); err != nil {
			return fmt.Errorf("recovery: failed to undo trxId=%d: %w", trxId, err)
		}
	}
	return nil
}

// collectUndoRecords は UNDO ページを走査して指定トランザクションのレコードを収集する
func (r *Recovery) collectUndoRecords(trxId uint64) []undoRecordEntry {
	pageNum := page.PageNumber(0)
	var records []undoRecordEntry

	for {
		pageId := page.NewPageId(r.undoFileId, pageNum)
		readData, err := r.bufferPool.GetReadPageData(pageId)
		if err != nil {
			break // ページが存在しない = 走査終了
		}

		undoPage := access.NewUndoPage(page.NewPage(readData))
		offset := 0
		for offset < int(undoPage.UsedBytes()) {
			recordBytes := undoPage.RecordAt(offset)
			if recordBytes == nil {
				break
			}

			recTrxId, _, recordType, tableName, columnSets, deserializeErr := access.DeserializeUndoRecord(recordBytes)
			if deserializeErr != nil {
				break
			}

			if recTrxId == trxId {
				records = append(records, undoRecordEntry{
					recordType: recordType,
					tableName:  tableName,
					columns:    columnSets,
				})
			}

			offset += len(recordBytes)
		}

		nextPageNum := undoPage.NextPageNumber()
		if nextPageNum == 0 {
			break
		}
		pageNum = page.PageNumber(nextPageNum)
	}

	return records
}

// buildTable はカタログからテーブルメタデータを取得し、リカバリ用の Table を構築する
func (r *Recovery) buildTable(tableName string) (*access.Table, error) {
	tblMeta, ok := r.catalog.GetTableMetaByName(tableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found in catalog", tableName)
	}

	var uniqueIndexes []*access.UniqueIndex
	for _, idxMeta := range tblMeta.Indexes {
		if idxMeta.Type == dictionary.IndexTypeUnique {
			colMeta, ok := tblMeta.GetColByName(idxMeta.ColName)
			if !ok {
				return nil, fmt.Errorf("column %s not found in table %s", idxMeta.ColName, tableName)
			}
			ui := access.NewUniqueIndex(idxMeta.Name, idxMeta.ColName, idxMeta.DataMetaPageId, colMeta.Pos, tblMeta.PKCount)
			uniqueIndexes = append(uniqueIndexes, ui)
		}
	}

	// undoLog=nil, redoLog=nil: リカバリ中に新たな UNDO/REDO を記録しない
	tbl := access.NewTable(tblMeta.Name, tblMeta.DataMetaPageId, tblMeta.PKCount, uniqueIndexes, nil, nil)
	return &tbl, nil
}

// undoRecordEntry はデシリアライズ済みの UNDO レコードの情報を保持する
type undoRecordEntry struct {
	recordType access.UndoRecordType
	tableName  string
	columns    [][][]byte
}

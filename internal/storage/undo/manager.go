package undo

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

// Entry は Undo ログのエントリ
type Entry struct {
	TrxId      lock.TrxId
	RecordType recordType
	Record     Record
}

// Manager は全トランザクションの Undo レコードをトランザクションごとに管理する
type Manager struct {
	bufferPool    *buffer.Pool
	redoLog       *redo.Buffer
	undoFileId    page.FileId            // Undo ファイルの FileId
	currentPageId page.Id                // 現在書き込み中の Undo ページ
	entries       map[lock.TrxId][]Entry // trxId → Entry[] のマップ
}

func NewManager(bp *buffer.Pool, redo *redo.Buffer, undoFileId page.FileId) (*Manager, error) {
	// Undo ページを割り当て
	pageId, err := bp.AllocatePageId(undoFileId)
	if err != nil {
		return nil, err
	}
	_, err = bp.AddPage(pageId)
	if err != nil {
		return nil, err
	}
	bufPageUndo, err := bp.PageForWrite(pageId)
	if err != nil {
		return nil, err
	}
	NewPage(*bufPageUndo.Page).Initialize()

	return &Manager{
		bufferPool:    bp,
		redoLog:       redo,
		undoFileId:    undoFileId,
		currentPageId: pageId,
		entries:       make(map[lock.TrxId][]Entry),
	}, nil
}

// Append は指定した trxId の Undo ログにレコードを追加し、書き込み先の Pointer を返す
func (m *Manager) Append(trxId lock.TrxId, recordType recordType, record Record) (Pointer, error) {
	ptr, err := m.writeToPage(trxId, record)
	if err != nil {
		return Pointer{}, err
	}
	m.entries[trxId] = append(m.entries[trxId], Entry{
		TrxId:      trxId,
		RecordType: recordType,
		Record:     record,
	})
	return ptr, nil
}

// Records は指定した trxId の Undo ログレコードを取得する
func (m *Manager) Records(trxId lock.TrxId) []Record {
	entries := m.entries[trxId]
	if len(entries) == 0 {
		return nil
	}
	records := make([]Record, len(entries))
	for i, e := range entries {
		records[i] = e.Record
	}
	return records
}

// CommittedEntries はコミット済みトランザクションの Undo エントリを返す
// (INSERT のエントリはコミット時に破棄済みのため、UPDATE/DELETE のみ含まれる)
func (m *Manager) CommittedEntries(committedTrxIds []lock.TrxId) []Entry {
	var result []Entry
	for _, trxId := range committedTrxIds {
		result = append(result, m.entries[trxId]...)
	}
	return result
}

// Discard は指定した trxId の Undo ログをすべて破棄する
func (m *Manager) Discard(trxId lock.TrxId) {
	delete(m.entries, trxId)
}

// DiscardRecordType は指定した trxId の指定したレコードタイプの Undo レコードのみ破棄する
func (m *Manager) DiscardRecordType(trxId lock.TrxId, recordType recordType) {
	entries := m.entries[trxId]
	kept := make([]Entry, 0, len(entries))
	for _, e := range entries {
		if e.RecordType != recordType {
			kept = append(kept, e)
		}
	}
	if len(kept) == 0 {
		delete(m.entries, trxId)
	} else {
		m.entries[trxId] = kept
	}
}

// writeToPage は Undo レコードを Undo ページに書き込み、書き込み先の Pointer を返す
func (m *Manager) writeToPage(trxId lock.TrxId, record Record) (Pointer, error) {
	undoNum := undoNumber(len(m.entries[trxId]))
	serialized := record.serialize(trxId, undoNum)

	pageUndo, err := m.bufferPool.PageForWrite(m.currentPageId)
	if err != nil {
		return Pointer{}, err
	}
	bufPageUndo := NewPage(*pageUndo.Page)

	ptr := NewPointer(m.currentPageId.PageNumber, bufPageUndo.UsedBytes())

	// ページが満杯の場合は、新しいページを割り当てる
	if !bufPageUndo.append(serialized) {
		newPageId, err := m.bufferPool.AllocatePageId(m.undoFileId)
		if err != nil {
			return Pointer{}, err
		}

		// 現在のページに次のページへのリンクを設定
		bufPageUndo.setNextPageNumber(newPageId.PageNumber)

		// 新しいページを初期化してレコードを追記
		_, err = m.bufferPool.AddPage(newPageId)
		if err != nil {
			return Pointer{}, err
		}
		pageNewUndo, err := m.bufferPool.PageForWrite(newPageId)
		if err != nil {
			return Pointer{}, err
		}
		newBufPageUndo := NewPage(*pageNewUndo.Page)
		newBufPageUndo.Initialize()

		ptr = Pointer{
			pageNumber: newPageId.PageNumber,
			offset:     0,
		}
		if !newBufPageUndo.append(serialized) {
			return Pointer{}, errors.New("undo: record too large for a single page")
		}
		m.currentPageId = newPageId
	}

	// Redo ログに Undo ページの変更を記録
	if m.redoLog != nil {
		pageUndo, err := m.bufferPool.PageForRead(m.currentPageId)
		if err != nil {
			return Pointer{}, err
		}
		m.redoLog.AppendPageCopy(trxId, m.currentPageId, *pageUndo.Page)
	}
	return ptr, nil
}

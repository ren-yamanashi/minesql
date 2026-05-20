package undo

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type entry struct {
	recordType RecordType
	record     Record
}

// Manager は全トランザクションの Undo レコードをトランザクションごとに管理する
type Manager struct {
	bufferPool    *buffer.BufferPool
	undoFileId    page.FileId            // Undo ファイルの FileId
	currentPageId page.PageId            // 現在書き込み中の Undo ページ
	entries       map[lock.TrxId][]entry // trxId → entry[] のマップ
}

func NewManager(bp *buffer.BufferPool, undoFileId page.FileId) (*Manager, error) {
	// Undo ページを割り当て
	pageId, err := bp.AllocatePageId(undoFileId)
	if err != nil {
		return nil, err
	}
	_, err = bp.AddPage(pageId)
	if err != nil {
		return nil, err
	}
	pageUndo, err := bp.GetWritePage(pageId)
	if err != nil {
		return nil, err
	}
	NewPage(*pageUndo).Initialize()

	return &Manager{
		bufferPool:    bp,
		undoFileId:    undoFileId,
		currentPageId: pageId,
		entries:       make(map[lock.TrxId][]entry),
	}, nil
}

// Append は指定した trxId の Undo ログにレコードを追加し、書き込み先の Pointer を返す
func (m *Manager) Append(trxId lock.TrxId, recordType RecordType, record Record) (Pointer, error) {
	ptr, err := m.writeToPage(trxId, record)
	if err != nil {
		return Pointer{}, nil
	}
	m.entries[trxId] = append(m.entries[trxId], entry{recordType: recordType, record: record})
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
		records[i] = e.record
	}
	return records
}

// PopLast は指定した trxId の Undo ログの最後のレコードを削除する
func (m *Manager) PopLast(trxId lock.TrxId) {
	entries := m.entries[trxId]
	if len(entries) > 0 {
		m.entries[trxId] = entries[:len(entries)-1]
	}
}

// Discard は指定した trxId の Undo ログをすべて破棄する (ROLLBACK 用)
func (m *Manager) Discard(trxId lock.TrxId) {
	delete(m.entries, trxId)
}

// writeToPage は Undo レコードを Undo ページに書き込み、書き込み先の Pointer を返す
func (m *Manager) writeToPage(trxId lock.TrxId, record Record) (Pointer, error) {
	undoNum := UndoNumber(len(m.entries[trxId]))
	serialized := record.Serialize(trxId, undoNum)

	pageUndo, err := m.bufferPool.GetWritePage(m.currentPageId)
	if err != nil {
		return Pointer{}, err
	}
	undoPage := NewPage(*pageUndo)

	ptr := newPointer(page.PageNumber(m.currentPageId.PageNumber), undoPage.UsedBytes())

	// ページが満杯の場合は、新しいページを割り当てる
	if !undoPage.Append(serialized) {
		newPageId, err := m.bufferPool.AllocatePageId(m.undoFileId)
		if err != nil {
			return Pointer{}, err
		}

		// 現在のページに次のページへのリンクを設定
		undoPage.SetNextPageNumber(newPageId.PageNumber)

		// 新しいページを初期化してレコードを追記
		pageNewUndo, err := m.bufferPool.GetWritePage(newPageId)
		if err != nil {
			return Pointer{}, nil
		}
		newUndoPage := NewPage(*pageNewUndo)
		newUndoPage.Initialize()

		ptr = Pointer{
			PageNumber: newPageId.PageNumber,
			Offset:     0,
		}
		newUndoPage.Append(serialized)
		m.currentPageId = newPageId
	}

	// TODO: Redo ログに Undo ページの変更を記録
	return ptr, nil
}

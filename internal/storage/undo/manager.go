package undo

import (
	"errors"
	"slices"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

var ErrRecordTooLarge = errors.New("undo: record too large for a single page")

type Manager struct {
	mu            sync.Mutex
	bufferPool    *buffer.Pool
	redoLog       *redo.Buffer
	fileId        page.FileId            // Undo ファイルの FileId
	currentPageId page.Id                // 現在書き込み中の Undo ページ
	entries       map[lock.TrxId][]Entry // trxId → Entry[] のマップ
}

func NewManager(bp *buffer.Pool, redoLog *redo.Buffer, fileId page.FileId) (*Manager, error) {
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()

	pageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		return nil, err
	}
	if _, err := bp.AddPage(pageId); err != nil {
		return nil, err
	}
	bufPageUndo, err := mtr.PageForWrite(pageId)
	if err != nil {
		return nil, err
	}
	CreatePage(*bufPageUndo.Data())

	return &Manager{
		bufferPool:    bp,
		redoLog:       redoLog,
		fileId:        fileId,
		currentPageId: pageId,
		entries:       make(map[lock.TrxId][]Entry),
	}, nil
}

// Append は指定した trxId の Undo ログにレコードを追加し、書き込み先の Pointer を返す
func (m *Manager) Append(mtr *buffer.Mtr, trxId lock.TrxId, recordType RecordType, record Record) (Pointer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ptr, err := m.writeToPage(mtr, trxId, record)
	if err != nil {
		return Pointer{}, err
	}
	m.entries[trxId] = append(m.entries[trxId], NewEntry(trxId, recordType, record))
	return ptr, nil
}

// Records は指定した trxId の Undo ログレコードを取得する
func (m *Manager) Records(trxId lock.TrxId) []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
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

// CommittedEntries はコミット済みトランザクションの Undo エントリを返す
func (m *Manager) CommittedEntries(committedTrxIds []lock.TrxId) []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []Entry
	for _, trxId := range committedTrxIds {
		result = append(result, m.entries[trxId]...)
	}
	return result
}

// Discard は指定した trxId の Undo ログをすべて破棄する
func (m *Manager) Discard(trxId lock.TrxId) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, trxId)
}

// DiscardRecordType は指定した trxId の指定したレコードタイプの Undo レコードのみ破棄する
func (m *Manager) DiscardRecordType(trxId lock.TrxId, recordType RecordType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := m.entries[trxId]
	kept := slices.DeleteFunc(entries, func(e Entry) bool {
		return e.recordType == recordType
	})
	if len(kept) == 0 {
		delete(m.entries, trxId)
	} else {
		m.entries[trxId] = kept
	}
}

// writeToPage は Undo レコードを Undo ページに書き込み、書き込み先の Pointer を返す
func (m *Manager) writeToPage(mtr *buffer.Mtr, trxId lock.TrxId, record Record) (Pointer, error) {
	undoNum := UndoNumber(len(m.entries[trxId]))
	serialized := record.Serialize(trxId, undoNum)

	pageUndo, err := mtr.PageForWrite(m.currentPageId)
	if err != nil {
		return Pointer{}, err
	}
	bufPageUndo := NewPage(*pageUndo.Data())

	// ページが満杯の場合は新しいページに切り替える (switchToNewPage 内で Redo 記録まで完了)
	if bufPageUndo.FreeSpace() < len(serialized) {
		return m.switchToNewPage(mtr, trxId, bufPageUndo, serialized)
	}

	prevUsedBytes := bufPageUndo.UsedBytes()
	if !bufPageUndo.append(serialized) {
		return Pointer{}, ErrRecordTooLarge
	}
	if err := m.appendRedoLog(mtr, trxId, m.currentPageId); err != nil {
		bufPageUndo.setUsedBytes(prevUsedBytes)
		return Pointer{}, err
	}
	return NewPointer(m.currentPageId.PageNumber(), prevUsedBytes), nil
}

// switchToNewPage は現在のページが満杯のとき、新しい Undo ページを割り当ててレコードを書き込む
func (m *Manager) switchToNewPage(
	mtr *buffer.Mtr,
	trxId lock.TrxId,
	currentPage *Page,
	serialized []byte,
) (Pointer, error) {
	newPageId, err := m.bufferPool.AllocatePageId(m.fileId)
	if err != nil {
		return Pointer{}, err
	}
	if _, err := m.bufferPool.AddPage(newPageId); err != nil {
		return Pointer{}, err
	}
	pageNewUndo, err := mtr.PageForWrite(newPageId)
	if err != nil {
		return Pointer{}, err
	}
	newBufPageUndo := CreatePage(*pageNewUndo.Data())
	if !newBufPageUndo.append(serialized) {
		return Pointer{}, ErrRecordTooLarge
	}

	if err := m.appendRedoLog(mtr, trxId, newPageId); err != nil {
		return Pointer{}, err
	}

	prevNextPageNumber := currentPage.NextPageNumber()
	currentPage.setNextPageNumber(newPageId.PageNumber())
	if err := m.appendRedoLog(mtr, trxId, m.currentPageId); err != nil {
		currentPage.setNextPageNumber(prevNextPageNumber)
		return Pointer{}, err
	}

	m.currentPageId = newPageId
	return NewPointer(newPageId.PageNumber(), 0), nil
}

// appendRedoLog は指定された Undo ページの Redo ログを記録する
//   - 呼び出し側が pageId に対して既に X latch を取得済みであることを前提とする
func (m *Manager) appendRedoLog(mtr *buffer.Mtr, trxId lock.TrxId, pageId page.Id) error {
	if m.redoLog == nil {
		return nil
	}
	pageUndo, err := mtr.PageForRead(pageId)
	if err != nil {
		return err
	}
	if _, err := m.redoLog.AppendPageCopy(trxId, pageId, pageUndo.Data()); err != nil {
		return err
	}
	return nil
}

package undo

import (
	"errors"
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

var errRecordTooLarge = errors.New("undo: record too large for a single page")

type Entry struct {
	TrxId      lock.TrxId
	RecordType RecordType
	Record     Record
}

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
	NewPage(*bufPageUndo.Data()).initialize()

	return &Manager{
		bufferPool:    bp,
		redoLog:       redo,
		undoFileId:    undoFileId,
		currentPageId: pageId,
		entries:       make(map[lock.TrxId][]Entry),
	}, nil
}

// Append は指定した trxId の Undo ログにレコードを追加し、書き込み先の Pointer を返す
func (m *Manager) Append(trxId lock.TrxId, recordType RecordType, record Record) (Pointer, error) {
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
	result := []Entry{}
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
func (m *Manager) DiscardRecordType(trxId lock.TrxId, recordType RecordType) {
	entries := m.entries[trxId]
	kept := slices.DeleteFunc(entries, func(e Entry) bool {
		return e.RecordType == recordType
	})
	if len(kept) == 0 {
		delete(m.entries, trxId)
	} else {
		m.entries[trxId] = kept
	}
}

// writeToPage は Undo レコードを Undo ページに書き込み、書き込み先の Pointer を返す
func (m *Manager) writeToPage(trxId lock.TrxId, record Record) (Pointer, error) {
	undoNum := UndoNumber(len(m.entries[trxId]))
	serialized := record.Serialize(trxId, undoNum)

	pageUndo, err := m.bufferPool.PageForWrite(m.currentPageId)
	if err != nil {
		return Pointer{}, err
	}
	bufPageUndo := NewPage(*pageUndo.Data())

	ptr := NewPointer(m.currentPageId.PageNumber, bufPageUndo.UsedBytes())

	// ページが満杯の場合は、新しいページを割り当てる
	if !bufPageUndo.append(serialized) {
		ptr, err = m.switchToNewPage(trxId, bufPageUndo, serialized)
		if err != nil {
			return Pointer{}, err
		}
	}

	if err := m.appendRedoLog(trxId); err != nil {
		return Pointer{}, err
	}
	return ptr, nil
}

// switchToNewPage は現在のページが満杯のとき、新しい Undo ページを割り当ててレコードを書き込む
func (m *Manager) switchToNewPage(trxId lock.TrxId, currentPage *Page, serialized []byte) (Pointer, error) {
	newPageId, err := m.bufferPool.AllocatePageId(m.undoFileId)
	if err != nil {
		return Pointer{}, err
	}

	// 現在のページに次のページへのリンクを設定
	currentPage.setNextPageNumber(newPageId.PageNumber)

	// 旧ページの Redo ログを記録 (nextPageNumber の変更を反映)
	if err := m.appendRedoLog(trxId); err != nil {
		return Pointer{}, err
	}

	// 新しいページを初期化してレコードを追記
	_, err = m.bufferPool.AddPage(newPageId)
	if err != nil {
		return Pointer{}, err
	}
	pageNewUndo, err := m.bufferPool.PageForWrite(newPageId)
	if err != nil {
		return Pointer{}, err
	}
	newBufPageUndo := NewPage(*pageNewUndo.Data())
	newBufPageUndo.initialize()

	if !newBufPageUndo.append(serialized) {
		return Pointer{}, errRecordTooLarge
	}
	m.currentPageId = newPageId

	return NewPointer(newPageId.PageNumber, 0), nil
}

// appendRedoLog は現在の Undo ページの Redo ログを記録する
func (m *Manager) appendRedoLog(trxId lock.TrxId) error {
	if m.redoLog == nil {
		return nil
	}
	pageUndo, err := m.bufferPool.PageForRead(m.currentPageId)
	if err != nil {
		return err
	}
	m.redoLog.AppendPageCopy(trxId, m.currentPageId, *pageUndo.Data())
	return nil
}

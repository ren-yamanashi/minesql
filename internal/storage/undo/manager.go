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

var (
	ErrRecordTooLarge = errors.New("undo: record too large for a single page")
	ErrNullPointer    = errors.New("undo: null pointer")
)

type Manager struct {
	mu            sync.Mutex
	bufferPool    *buffer.Pool
	fileId        page.FileId            // Undo ファイルの FileId
	currentPageId page.Id                // 現在書き込み中の Undo ページ
	entries       map[lock.TrxId][]Entry // trxId → Entry[] のマップ
}

func NewManager(bp *buffer.Pool, fileId page.FileId, redoLog *redo.Buffer) (*Manager, error) {
	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, redoLog)

	pageId, err := bp.AllocatePageId(fileId)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	if _, err := bp.AddPage(pageId); err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	bufPageUndo, err := mtr.PageForWrite(pageId)
	if err != nil {
		mtr.UnpinAll()
		return nil, err
	}
	CreatePage(bufPageUndo)
	if err := mtr.Commit(); err != nil {
		return nil, err
	}

	return &Manager{
		bufferPool:    bp,
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

// LookupByPointer は Undo ポインタが指す Undo レコードを返す
//   - ptr が NullPointer の場合は ErrNullPointer を返す
func (m *Manager) LookupByPointer(mtr *buffer.Mtr, ptr Pointer) (Record, error) {
	if ptr.IsNull() {
		return nil, ErrNullPointer
	}

	pageId := page.NewId(m.fileId, ptr.pageNumber)
	pageUndo, err := mtr.PageForRead(pageId)
	if err != nil {
		return nil, err
	}
	undoPage := NewPage(pageUndo)

	recordBytes := undoPage.Record(int(ptr.offset))
	if recordBytes == nil {
		return nil, ErrInvalidRecord
	}
	fields, err := DeserializeFields(recordBytes)
	if err != nil {
		return nil, err
	}
	return fields.ToRecord()
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
	bufPageUndo := NewPage(pageUndo)

	// ページが満杯の場合は新しいページに切り替えてレコードを書き込む
	if bufPageUndo.FreeSpace() < len(serialized) {
		return m.switchToNewPage(mtr, bufPageUndo, serialized)
	}

	prevUsedBytes := bufPageUndo.UsedBytes()
	if !bufPageUndo.append(serialized) {
		return Pointer{}, ErrRecordTooLarge
	}
	pageId := m.currentPageId
	// データページ (この後 btree が変更する) より先に Undo ページの Redo を記録するためここで Unpin する
	mtr.Unpin(pageId)
	return NewPointer(pageId.PageNumber(), prevUsedBytes), nil
}

// switchToNewPage は現在のページが満杯のとき、新しい Undo ページを割り当ててレコードを書き込む
//   - 新ページへの書き込み (実体) を先に行い、その後に旧ページの次ページリンクを更新する
//   - 新ページ→旧ページの順で Unpin し、その順序で Redo 記録する (どちらもデータページより前に記録される)
func (m *Manager) switchToNewPage(
	mtr *buffer.Mtr,
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
	newBufPageUndo := CreatePage(pageNewUndo)
	if !newBufPageUndo.append(serialized) {
		return Pointer{}, ErrRecordTooLarge
	}

	oldPageId := m.currentPageId
	currentPage.setNextPageNumber(newPageId.PageNumber())
	m.currentPageId = newPageId

	// 新ページの実体 → 旧ページのリンク更新の順で記録する
	mtr.Unpin(newPageId)
	mtr.Unpin(oldPageId)
	return NewPointer(newPageId.PageNumber(), 0), nil
}

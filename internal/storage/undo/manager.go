package undo

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
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
	redoLog       *redo.Buffer           // Undo 書き込みの mini-transaction が記録する Redo ログ参照
	fileId        page.FileId            // Undo ファイルの FileId
	currentPageId page.Id                // 現在書き込み中の Undo ページ
	entries       map[lock.TrxId][]Entry // trxId → Entry[] のマップ
}

// NewManager は Undo ファイルの FSP ヘッダーを初期化し、先頭ページを 1 枚確保して Manager を返す
//   - mtr: FSP ヘッダー初期化と先頭ページ初期化を記録する Mtr。Commit / UnpinAll は呼び出し側
//   - 先頭ページは PageNumber == 1 で確保される
func NewManager(mtr *buffer.Mtr, fileId page.FileId) (*Manager, error) {
	if err := fsp.InitHeader(mtr, fileId); err != nil {
		return nil, err
	}
	bp := mtr.Pool()
	pageId, err := fsp.AllocatePage(mtr, fileId)
	if err != nil {
		return nil, err
	}
	if pageId.PageNumber() != 1 {
		panic(fmt.Sprintf("undo: first allocated page must be PageNumber 1, got %d", pageId.PageNumber()))
	}
	if _, err := bp.AddPage(pageId); err != nil {
		return nil, err
	}
	bufPageUndo, err := mtr.PageForWrite(pageId)
	if err != nil {
		return nil, err
	}
	CreatePage(bufPageUndo)

	return &Manager{
		bufferPool:    bp,
		redoLog:       mtr.Redo(),
		fileId:        fileId,
		currentPageId: pageId,
		entries:       make(map[lock.TrxId][]Entry),
	}, nil
}

// Append は指定した trxId の Undo ログにレコードを追加し、書き込み先の Pointer を返す
//   - Undo ページ書き込みは Undo 専用の mini-transaction として独立して commit される
//   - 呼び出し側のデータ操作 mini-transaction は、返された Pointer をレコードに記録するだけでよい
func (m *Manager) Append(trxId lock.TrxId, recordType RecordType, record Record) (Pointer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	mtr := buffer.NewWriteMtr(m.bufferPool, trxId, m.redoLog)
	ptr, err := m.writeToPage(mtr, trxId, record)
	if err != nil {
		mtr.UnpinAll()
		return Pointer{}, err
	}
	if err := mtr.Commit(); err != nil {
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
	return NewPointer(m.currentPageId.PageNumber(), prevUsedBytes), nil
}

// switchToNewPage は現在のページが満杯のとき、新しい Undo ページを割り当ててレコードを書き込む
func (m *Manager) switchToNewPage(
	mtr *buffer.Mtr,
	currentPage *Page,
	serialized []byte,
) (Pointer, error) {
	newPageId, err := fsp.AllocatePage(mtr, m.fileId)
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

	currentPage.setNextPageNumber(newPageId.PageNumber())
	m.currentPageId = newPageId

	return NewPointer(newPageId.PageNumber(), 0), nil
}

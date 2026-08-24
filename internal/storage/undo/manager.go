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

// NewManager は Undo ファイルの FSP ヘッダーを初期化し、 チェーン先頭ページを 1 枚確保して Manager を返す
//   - mtr: FSP ヘッダー初期化とチェーン先頭ページの segment 作成を記録する Mtr。Commit / UnpinAll は呼び出し側
//   - 先頭ページは PageNumber == ChainHeadPageNumber で確保される
//   - bootstrap 経路のため、途中失敗はすべて panic で扱う
func NewManager(mtr *buffer.Mtr, fileId page.FileId) *Manager {
	if err := fsp.InitHeader(mtr, fileId); err != nil {
		panic(fmt.Sprintf("undo: bootstrap failed to init header: %v", err))
	}
	bp := mtr.Pool()
	pageId := CreateChainRoot(mtr, fileId)
	if pageId.PageNumber() != ChainHeadPageNumber {
		panic(fmt.Sprintf("undo: chain head page must be PageNumber %d, got %d", ChainHeadPageNumber, pageId.PageNumber()))
	}

	return &Manager{
		bufferPool:    bp,
		redoLog:       mtr.Redo(),
		fileId:        fileId,
		currentPageId: pageId,
		entries:       make(map[lock.TrxId][]Entry),
	}
}

// Append は指定した trxId の Undo ログにレコードを追加し、書き込み先の Pointer を返す
//   - Undo ページ書き込みは Undo 専用の mini-transaction として独立して commit される
//   - 呼び出し側のデータ操作 mini-transaction は、返された Pointer をレコードに記録するだけでよい
//   - 単一 Undo ページに収まらないサイズのレコードは書き込み開始前に ErrRecordTooLarge で拒否する
func (m *Manager) Append(trxId lock.TrxId, recordType RecordType, record Record) (Pointer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	undoNum := UndoNumber(len(m.entries[trxId]))
	serialized := record.Serialize(trxId, undoNum)
	if len(serialized) > maxRecordSize {
		return Pointer{}, ErrRecordTooLarge
	}

	mtr := buffer.NewWriteMtr(m.bufferPool, trxId, m.redoLog)
	ptr, err := m.writeToPage(mtr, serialized)
	if err != nil {
		if commitErr := mtr.Commit(); commitErr != nil {
			return Pointer{}, commitErr
		}
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

// Count は指定した trxId の現在の Undo エントリ件数を返す
//   - 文レベル rollback の savepoint 値として利用する
func (m *Manager) Count(trxId lock.TrxId) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries[trxId])
}

// RecordsFrom は指定した trxId の from 位置以降の Undo レコードを返す
//   - from は Count で取得した savepoint 値。from が現在の件数と等しい / 大きい場合は nil
func (m *Manager) RecordsFrom(trxId lock.TrxId, from int) []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := m.entries[trxId]
	if from >= len(entries) {
		return nil
	}
	tail := entries[from:]
	records := make([]Record, len(tail))
	for i, e := range tail {
		records[i] = e.record
	}
	return records
}

// DiscardFrom は指定した trxId の from 位置以降の Undo エントリを破棄する
//   - from が現在の件数と等しい / 大きい場合は no-op
//   - 破棄後にエントリが 0 件になった場合はマップから当該 trxId を削除する
func (m *Manager) DiscardFrom(trxId lock.TrxId, from int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := m.entries[trxId]
	if from >= len(entries) {
		return
	}
	if from <= 0 {
		delete(m.entries, trxId)
		return
	}
	m.entries[trxId] = entries[:from]
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
	undoPage := openUndoPage(pageUndo, pageId)

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

// writeToPage は事前検証済みの Undo レコードを Undo ページに書き込み、書き込み先の Pointer を返す
//   - serialized は Append 冒頭で maxRecordSize 以下であることが確認済みでなければならない
func (m *Manager) writeToPage(mtr *buffer.Mtr, serialized []byte) (Pointer, error) {
	pageUndo, err := mtr.PageForWrite(m.currentPageId)
	if err != nil {
		return Pointer{}, err
	}
	bufPageUndo := openUndoPage(pageUndo, m.currentPageId)

	// ページが満杯の場合は新しいページに切り替えてレコードを書き込む
	if bufPageUndo.FreeSpace() < len(serialized) {
		return m.switchToNewPage(mtr, bufPageUndo, serialized)
	}

	prevUsedBytes := bufPageUndo.UsedBytes()
	if !bufPageUndo.append(serialized) {
		panic(fmt.Sprintf("undo: append to current page failed after free space check (pageId=%v, size=%d)", m.currentPageId, len(serialized)))
	}
	return NewPointer(m.currentPageId.PageNumber(), prevUsedBytes), nil
}

// switchToNewPage は現在のページが満杯のとき、新しい Undo ページを割り当ててレコードを書き込む
//   - AddPage / PageForWrite に失敗した場合は割り当てを補償解放してから失敗を返す
//   - 補償解放の途中でさらに失敗した場合は続行不能な二重障害として panic する
func (m *Manager) switchToNewPage(
	mtr *buffer.Mtr,
	currentPage *Page,
	serialized []byte,
) (Pointer, error) {
	rootHeaderAt := chainRootHeaderAt(page.NewId(m.fileId, ChainHeadPageNumber))
	newPageId, err := fsp.AllocateSegmentPage(mtr, m.fileId, rootHeaderAt)
	if err != nil {
		return Pointer{}, err
	}
	if _, err := m.bufferPool.AddPage(newPageId); err != nil {
		if compErr := fsp.FreeSegmentPage(mtr, rootHeaderAt, newPageId); compErr != nil {
			panic(fmt.Sprintf("undo: page compensation failed after undo page allocation: %v", compErr))
		}
		return Pointer{}, err
	}
	pageNewUndo, err := mtr.PageForWrite(newPageId)
	if err != nil {
		if compErr := fsp.FreeSegmentPage(mtr, rootHeaderAt, newPageId); compErr != nil {
			panic(fmt.Sprintf("undo: page compensation failed after undo page allocation: %v", compErr))
		}
		return Pointer{}, err
	}
	newBufPageUndo := CreatePage(pageNewUndo)
	if !newBufPageUndo.append(serialized) {
		panic(fmt.Sprintf("undo: append to new page failed after size check (pageId=%v, size=%d)", newPageId, len(serialized)))
	}

	currentPage.setNextPageNumber(newPageId.PageNumber())
	m.currentPageId = newPageId

	return NewPointer(newPageId.PageNumber(), 0), nil
}

// openUndoPage は pageId のチェーン内位置 (先頭 or 後続) に応じた undo.Page ビューを返す
func openUndoPage(bufPage *buffer.Page, pageId page.Id) *Page {
	if pageId.PageNumber() == ChainHeadPageNumber {
		return NewFirstPage(bufPage)
	}
	return NewPage(bufPage)
}

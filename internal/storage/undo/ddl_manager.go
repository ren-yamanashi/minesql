package undo

import (
	"errors"
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// ErrInvalidDDLUndoRoot は DDLManager 構築時に無効な先頭 PageId が渡されたことを表す
var ErrInvalidDDLUndoRoot = errors.New("undo: invalid ddl undo root page id")

// DDLManager は DDL Undo 専用領域の書き込み・読み出し・破棄を担う
//   - 通常 Undo Manager と並列構造 (= 別の型として独立)
//   - 並行 DDL は想定しないため内部に mutex を持たない (= 単一スレッドで逐次操作される前提)
type DDLManager struct {
	bufferPool        *buffer.Pool
	fileId            page.FileId
	freeListMapPageId page.Id
	rootPageId        page.Id
	currentPageId     page.Id
}

// NewDDLManager は既存の DDL Undo 専用領域を開く
//   - rootPageId: カタログヘッダーが指す DDL Undo 先頭ページ ID。 無効値が渡されると ErrInvalidDDLUndoRoot を返す
//   - freeListMapPageId: Clear 時に Pool.Deallocate へ渡すフリーリストマップページ ID
//   - 先頭ページから next リンクを辿って末尾ページを currentPageId に設定する
func NewDDLManager(
	bp *buffer.Pool,
	fileId page.FileId,
	rootPageId page.Id,
	freeListMapPageId page.Id,
) (*DDLManager, error) {
	if rootPageId.IsInvalid() {
		return nil, ErrInvalidDDLUndoRoot
	}
	m := &DDLManager{
		bufferPool:        bp,
		fileId:            fileId,
		freeListMapPageId: freeListMapPageId,
		rootPageId:        rootPageId,
	}
	pageId := rootPageId
	for {
		bufPage, err := bp.Page(pageId)
		if err != nil {
			return nil, err
		}
		ddlPage := NewPage(bufPage)
		nextPN := ddlPage.NextPageNumber()
		bp.Unpin(pageId)
		if nextPN == 0 {
			m.currentPageId = pageId
			return m, nil
		}
		pageId = page.NewId(fileId, nextPN)
	}
}

// Append は DDL Undo レコードを末尾ページに追加する
//   - ページが満杯の場合は新ページを Allocate して next リンクで繋ぐ
//   - 全ての書き込みは mtr 経由で行われ Redo に記録される
func (m *DDLManager) Append(mtr *buffer.Mtr, record DDLRecord) error {
	serialized := record.Serialize()

	pageUndo, err := mtr.PageForWrite(m.currentPageId)
	if err != nil {
		return err
	}
	ddlPage := NewPage(pageUndo)

	if ddlPage.FreeSpace() < len(serialized) {
		return m.switchToNewPage(mtr, ddlPage, serialized)
	}

	if !ddlPage.append(serialized) {
		return ErrRecordTooLarge
	}
	return nil
}

// ReverseScan は DDL Undo レコードを後から積まれた順 (= 逆順) に列挙する
//   - 先頭ページから next リンクを辿って全レコードを順方向に収集してから反転する
func (m *DDLManager) ReverseScan(mtr *buffer.Mtr) ([]DDLRecord, error) {
	var records []DDLRecord
	pageId := m.rootPageId
	for {
		bufPage, err := mtr.PageForRead(pageId)
		if err != nil {
			return nil, err
		}
		ddlPage := NewPage(bufPage)
		used := int(ddlPage.UsedBytes())
		offset := 0
		for offset < used {
			rec, consumed, decodeErr := DeserializeDDLRecord(ddlPage.BodyAt(offset, used-offset))
			if decodeErr != nil {
				mtr.Unpin(pageId)
				return nil, decodeErr
			}
			records = append(records, rec)
			offset += consumed
		}
		nextPN := ddlPage.NextPageNumber()
		mtr.Unpin(pageId)
		if nextPN == 0 {
			break
		}
		pageId = page.NewId(m.fileId, nextPN)
	}
	slices.Reverse(records)
	return records, nil
}

// Clear は DDLManager が管理する DDL Undo 領域コンテナの中身を空にする
//   - root ページは残し (= 永続コンテナ)、 中間ページ (= root の next 以降) があれば逆順で Pool.Deallocate する
//   - root ページの UsedBytes と NextPageNumber を 0 にリセットし、 次回 Append で再利用可能にする
//   - 内部状態 currentPageId は rootPageId に戻る
//   - 全ての書き込みは mtr 経由 (Redo に記録される)
func (m *DDLManager) Clear(mtr *buffer.Mtr) error {
	var intermediatePageIds []page.Id
	pageId := m.rootPageId
	for {
		bufPage, err := mtr.PageForRead(pageId)
		if err != nil {
			return err
		}
		ddlPage := NewPage(bufPage)
		nextPN := ddlPage.NextPageNumber()
		mtr.Unpin(pageId)
		if nextPN == 0 {
			break
		}
		pageId = page.NewId(m.fileId, nextPN)
		intermediatePageIds = append(intermediatePageIds, pageId)
	}

	for i := len(intermediatePageIds) - 1; i >= 0; i-- {
		if err := m.bufferPool.Deallocate(mtr, m.freeListMapPageId, intermediatePageIds[i]); err != nil {
			return err
		}
	}

	rootBufPage, err := mtr.PageForWrite(m.rootPageId)
	if err != nil {
		return err
	}
	CreatePage(rootBufPage)

	m.currentPageId = m.rootPageId
	return nil
}

// switchToNewPage は満杯ページの代わりに新ページを確保してレコードを書き込み、 旧ページの next リンクを更新する
//   - 新ページの実体化 → 旧ページのリンク変更の順で Unpin (= Redo 記録) する
func (m *DDLManager) switchToNewPage(
	mtr *buffer.Mtr,
	currentPage *Page,
	serialized []byte,
) error {
	newPageId, err := m.bufferPool.AllocatePageId(m.fileId)
	if err != nil {
		return err
	}
	if _, err := m.bufferPool.AddPage(newPageId); err != nil {
		return err
	}
	pageNewUndo, err := mtr.PageForWrite(newPageId)
	if err != nil {
		return err
	}
	newDDLPage := CreatePage(pageNewUndo)
	if !newDDLPage.append(serialized) {
		return ErrRecordTooLarge
	}

	currentPage.setNextPageNumber(newPageId.PageNumber())
	m.currentPageId = newPageId

	return nil
}

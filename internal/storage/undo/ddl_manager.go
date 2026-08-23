package undo

import (
	"errors"
	"fmt"
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

// ErrInvalidDDLUndoRoot は DDLManager 構築時に無効な先頭 PageId が渡されたことを表す
var ErrInvalidDDLUndoRoot = errors.New("undo: invalid ddl undo root page id")

// DDLManager は DDL Undo 専用領域の書き込み・読み出し・破棄を担う
//   - 通常 Undo Manager と並列構造 (= 別の型として独立)
//   - 並行 DDL は想定しないため内部に mutex を持たない (= 単一スレッドで逐次操作される前提)
type DDLManager struct {
	bufferPool    *buffer.Pool
	fileId        page.FileId
	rootPageId    page.Id
	currentPageId page.Id
}

// NewDDLManager は既存の DDL Undo 専用領域を開く
//   - bp: DDL Undo ページを載せるバッファプール
//   - rootPageId: カタログヘッダーが指す DDL Undo 先頭ページ ID。 無効値が渡されると ErrInvalidDDLUndoRoot を返す
func NewDDLManager(
	bp *buffer.Pool,
	fileId page.FileId,
	rootPageId page.Id,
) (*DDLManager, error) {
	if rootPageId.IsInvalid() {
		return nil, ErrInvalidDDLUndoRoot
	}
	m := &DDLManager{
		bufferPool: bp,
		fileId:     fileId,
		rootPageId: rootPageId,
	}
	pageId := rootPageId
	for {
		bufPage, err := bp.Page(pageId)
		if err != nil {
			return nil, err
		}
		ddlPage := openDDLPage(bufPage, pageId, rootPageId)
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
//   - 単一 Undo ページに収まらないサイズのレコードは書き込み開始前に ErrRecordTooLarge で拒否する
func (m *DDLManager) Append(mtr *buffer.Mtr, record DDLRecord) error {
	serialized := record.Serialize()
	if len(serialized) > maxRecordSize {
		return ErrRecordTooLarge
	}

	pageUndo, err := mtr.PageForWrite(m.currentPageId)
	if err != nil {
		return err
	}
	ddlPage := openDDLPage(pageUndo, m.currentPageId, m.rootPageId)

	if ddlPage.FreeSpace() < len(serialized) {
		return m.switchToNewPage(mtr, ddlPage, serialized)
	}

	if !ddlPage.append(serialized) {
		panic(fmt.Sprintf("undo: append to current DDL page failed after free space check (pageId=%v, size=%d)", m.currentPageId, len(serialized)))
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
		ddlPage := openDDLPage(bufPage, pageId, m.rootPageId)
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

// Clear は DDL Undo 領域コンテナの中身を空にする
//   - root ページは残し、中間ページを末尾から 1 ページずつ独立 mtr で解放する
//   - 最後に root ページの UsedBytes / NextPageNumber を独立 mtr で 0 にリセットする
//   - redoLog が非 nil の場合は trxId 配下の書き込み mtr として Redo に記録する
//   - redoLog が nil の場合は Redo に記録せずバッファページを dirty 化するだけで返る
//     (リカバリ経路用。 ページ効果はリカバリ完了処理の一括フラッシュで永続化される)
func (m *DDLManager) Clear(trxId lock.TrxId, redoLog *redo.Buffer) error {
	pageIds, err := m.collectChainPageIds()
	if err != nil {
		return err
	}
	rootHeaderAt := chainRootHeaderAt(m.rootPageId)
	for i := len(pageIds) - 1; i >= 1; i-- {
		if err := m.freeTailPage(trxId, redoLog, rootHeaderAt, pageIds[i-1], pageIds[i]); err != nil {
			return err
		}
		m.currentPageId = pageIds[i-1]
	}
	if err := m.resetRoot(trxId, redoLog); err != nil {
		return err
	}
	m.currentPageId = m.rootPageId
	return nil
}

func (m *DDLManager) collectChainPageIds() ([]page.Id, error) {
	mtr := buffer.NewMtr(m.bufferPool)
	defer mtr.UnpinAll()
	pageIds := []page.Id{m.rootPageId}
	pageId := m.rootPageId
	for {
		bufPage, err := mtr.PageForRead(pageId)
		if err != nil {
			return nil, err
		}
		nextPN := openDDLPage(bufPage, pageId, m.rootPageId).NextPageNumber()
		if nextPN == 0 {
			return pageIds, nil
		}
		pageId = page.NewId(m.fileId, nextPN)
		pageIds = append(pageIds, pageId)
	}
}

func (m *DDLManager) freeTailPage(
	trxId lock.TrxId, redoLog *redo.Buffer, rootHeaderAt flst.Address, prevPageId, targetPageId page.Id,
) error {
	mtr := buffer.NewWriteMtr(m.bufferPool, trxId, redoLog)
	prevBufPage, err := mtr.PageForWrite(prevPageId)
	if err != nil {
		mtr.UnpinAll()
		return err
	}
	if err := fsp.FreeSegmentPage(mtr, rootHeaderAt, targetPageId); err != nil {
		mtr.UnpinAll()
		return err
	}
	openDDLPage(prevBufPage, prevPageId, m.rootPageId).setNextPageNumber(0)
	return mtr.Commit()
}

func (m *DDLManager) resetRoot(trxId lock.TrxId, redoLog *redo.Buffer) error {
	mtr := buffer.NewWriteMtr(m.bufferPool, trxId, redoLog)
	rootBufPage, err := mtr.PageForWrite(m.rootPageId)
	if err != nil {
		mtr.UnpinAll()
		return err
	}
	CreateFirstPage(rootBufPage)
	return mtr.Commit()
}

// switchToNewPage は満杯ページの代わりに新ページを確保してレコードを書き込み、 旧ページの next リンクを更新する
//   - 新ページの実体化 → 旧ページのリンク変更の順で Unpin (= Redo 記録) する
//   - AddPage / PageForWrite に失敗した場合は割り当てを補償解放してから失敗を返す
//   - 補償解放の途中でさらに失敗した場合は続行不能な二重障害として panic する
func (m *DDLManager) switchToNewPage(
	mtr *buffer.Mtr,
	currentPage *Page,
	serialized []byte,
) error {
	rootHeaderAt := chainRootHeaderAt(m.rootPageId)
	newPageId, err := fsp.AllocateSegmentPage(mtr, m.fileId, rootHeaderAt)
	if err != nil {
		return err
	}
	if _, err := m.bufferPool.AddPage(newPageId); err != nil {
		if compErr := fsp.FreeSegmentPage(mtr, rootHeaderAt, newPageId); compErr != nil {
			panic(fmt.Sprintf("undo: page compensation failed after ddl page allocation: %v", compErr))
		}
		return err
	}
	pageNewUndo, err := mtr.PageForWrite(newPageId)
	if err != nil {
		if compErr := fsp.FreeSegmentPage(mtr, rootHeaderAt, newPageId); compErr != nil {
			panic(fmt.Sprintf("undo: page compensation failed after ddl page allocation: %v", compErr))
		}
		return err
	}
	newDDLPage := CreatePage(pageNewUndo)
	if !newDDLPage.append(serialized) {
		panic(fmt.Sprintf("undo: append to new DDL page failed after size check (pageId=%v, size=%d)", newPageId, len(serialized)))
	}

	currentPage.setNextPageNumber(newPageId.PageNumber())
	m.currentPageId = newPageId

	return nil
}

// openDDLPage は DDL チェーン内位置 (rootPageId かそれ以外か) に応じた undo.Page ビューを返す
func openDDLPage(bufPage *buffer.Page, pageId, rootPageId page.Id) *Page {
	if pageId == rootPageId {
		return NewFirstPage(bufPage)
	}
	return NewPage(bufPage)
}

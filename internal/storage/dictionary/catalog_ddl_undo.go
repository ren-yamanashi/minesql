package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

func (c *Catalog) DDLUndoRootPageId() page.Id { return c.ddlUndoRootPageId }

// SetDDLUndoRootPageId はヘッダーページの DDL Undo 先頭 PageNumber を更新する
//   - pageId に page.InvalidId() を渡すと無効値 (page.MaxPageNumber) が書かれ、 内部状態も page.InvalidId() を保持する
//   - 書き込みは mtr 経由で行われ Redo に記録される
func (c *Catalog) SetDDLUndoRootPageId(mtr *buffer.Mtr, pageId page.Id) error {
	headerPageId := page.NewId(catalogFileId, catalogHeaderPageNum)
	bufPageHeader, err := mtr.PageForWrite(headerPageId)
	if err != nil {
		return err
	}
	writePageNumber(bufPageHeader, headerDDLUndoRootPageNumberOffset, pageId.PageNumber())
	c.ddlUndoRootPageId = pageId
	return nil
}

// ddlUndoRootPageIdFromPageNumber は永続化された PageNumber から DDL Undo 先頭 PageId を組み立てる
//   - page.MaxPageNumber は「未確保」を表す番兵値で、 page.InvalidId() にマップする
//   - それ以外は catalog ファイル内のページとして組み立てる
func ddlUndoRootPageIdFromPageNumber(pn page.PageNumber) page.Id {
	if pn == page.MaxPageNumber {
		return page.InvalidId()
	}
	return page.NewId(catalogFileId, pn)
}

// allocateAndInitializeDDLUndoRootPage は DDL Undo 専用領域の先頭ページを新規確保し、 空の Undo ページとして初期化する
//   - 呼び出し側はエラー時に mtr.UnpinAll() を行う既存パターンに従う
func allocateAndInitializeDDLUndoRootPage(mtr *buffer.Mtr, bp *buffer.Pool) (page.Id, error) {
	pageId, err := bp.AllocatePageId(catalogFileId)
	if err != nil {
		return page.InvalidId(), err
	}
	if _, err := bp.AddPage(pageId); err != nil {
		return page.InvalidId(), err
	}
	bufPage, err := mtr.PageForWrite(pageId)
	if err != nil {
		return page.InvalidId(), err
	}
	undo.CreatePage(bufPage)
	return pageId, nil
}

package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

func (c *Catalog) DDLUndoRootPageId() page.Id { return c.ddlUndoRootPageId }

// SetDDLUndoRootPageId はヘッダーページの DDL Undo 先頭 PageNumber を更新する
//   - 書き込みは mtr 経由で行われ Redo に記録される
func (c *Catalog) SetDDLUndoRootPageId(mtr *buffer.Mtr, pageId page.Id) error {
	headerPageId := page.NewId(CatalogFileId, catalogHeaderPageNum)
	bufPageHeader, err := mtr.PageForWrite(headerPageId)
	if err != nil {
		return err
	}
	writePageNumber(bufPageHeader, headerDDLUndoRootPageNumberOffset, pageId.PageNumber())
	c.ddlUndoRootPageId = pageId
	return nil
}

// ddlUndoRootPageIdFromPageNumber は永続化された PageNumber から DDL Undo 先頭 PageId を組み立てる
//   - 永続コンテナ型では DDL Undo 領域は CreateCatalog 時に 1 度確保されて以降ずっと有効値が入る
//   - page.MaxPageNumber (= 未確保) は通常運用では発生しないが、 互換のため page.InvalidId() にマップする
func ddlUndoRootPageIdFromPageNumber(pn page.PageNumber) page.Id {
	if pn == page.MaxPageNumber {
		return page.InvalidId()
	}
	return page.NewId(CatalogFileId, pn)
}

// allocateAndInitializeDDLUndoRootPage は DDL Undo 専用領域の先頭ページを新規確保し、 空の Undo ページとして初期化する
//   - 呼び出し側はエラー時に mtr.UnpinAll() を行う既存パターンに従う
func allocateAndInitializeDDLUndoRootPage(mtr *buffer.Mtr, bp *buffer.Pool) (page.Id, error) {
	pageId, err := fsp.AllocatePage(mtr, CatalogFileId)
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

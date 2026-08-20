package dictionary

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
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

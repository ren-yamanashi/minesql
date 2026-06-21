package buffer

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

const freeListNextPointerSize = 4

// Deallocate は指定 pageId をファイル内フリーリストに追加する
//   - mtr: 書き込み用 Mtr (Redo に記録される)
//   - freeListMapPageId: フリーリストマップページの PageId
//   - pageId: 解放対象ページ。 同ファイルのフリーリスト末尾につながれ、 マップページの該当 FileId エントリが新しい先頭として更新される
func (p *Pool) Deallocate(mtr *Mtr, freeListMapPageId page.Id, pageId page.Id) error {
	mapBufPage, err := mtr.PageForWrite(freeListMapPageId)
	if err != nil {
		return err
	}
	mapPage := newFreeListMapPage(mapBufPage)

	fileId := pageId.FileId()
	oldHead := mapPage.headPageNumber(fileId)

	deadBufPage, err := mtr.PageForWrite(pageId)
	if err != nil {
		return err
	}

	var buf [freeListNextPointerSize]byte
	binary.BigEndian.PutUint32(buf[:], uint32(oldHead))
	deadBufPage.WriteBodyAt(0, buf[:])

	mapPage.setHeadPageNumber(fileId, pageId.PageNumber())
	return nil
}

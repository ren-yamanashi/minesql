package buffer

import (
	"encoding/binary"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// HasHeapFile は指定 FileId が現在 BufferPool に登録されているかを返す
func (p *Pool) HasHeapFile(fileId page.FileId) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.files[fileId]
	return ok
}

// IsPageInFileFreeList は指定ページが freeListMapPageId 配下のフリーリスト (= target.FileId() のリスト) に含まれているかを返す
//   - mtr: 走査中のページ Pin に使う Mtr
//   - freeListMapPageId: 走査対象のフリーリストマップページ ID
//   - target: 所属を確認したいページ ID
//   - フリーリストの head から next ポインタを辿って線形に走査する
func (p *Pool) IsPageInFileFreeList(mtr *Mtr, freeListMapPageId page.Id, target page.Id) (bool, error) {
	mapBufPage, err := mtr.PageForRead(freeListMapPageId)
	if err != nil {
		return false, err
	}
	head := newFreeListMapPage(mapBufPage).headPageNumber(target.FileId())
	mtr.Unpin(freeListMapPageId)

	for current := head; current != page.MaxPageNumber; {
		if current == target.PageNumber() {
			return true, nil
		}
		currentPageId := page.NewId(target.FileId(), current)
		bufPage, err := mtr.PageForRead(currentPageId)
		if err != nil {
			return false, err
		}
		nextPN := page.PageNumber(binary.BigEndian.Uint32(bufPage.Data().Body()[0:freeListNextPointerSize]))
		mtr.Unpin(currentPageId)
		current = nextPN
	}
	return false, nil
}

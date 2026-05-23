package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// AddPage はバッファプールに新しいページを追加する
//
// バッファプールに空きがある場合は新しいページを追加し、空きがない場合は古いページを新しいページに置き換える
func (bp *Pool) AddPage(pageId page.Id) (*Page, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	return bp.addPage(pageId)
}

// addPage はバッファプールに新しいページを追加する
func (bp *Pool) addPage(pageId page.Id) (*Page, error) {
	// バッファプールに空きがある場合: 新しいバッファページを追加・ページテーブルを更新
	if len(bp.pages) < bp.MaxNumOfPage {
		newBufPage, err := newPage(pageId)
		if err != nil {
			return nil, err
		}
		bp.pages = append(bp.pages, *newBufPage)
		bufferId := id(len(bp.pages) - 1)
		bp.pageTable.add(pageId, bufferId)
		bp.lru.access(bufferId)
		return &bp.pages[bufferId], nil
	}

	// バッファプールに空きがない場合: ページを追い出す
	victimBufId := bp.lru.evict()
	victimBufPage := &bp.pages[victimBufId]

	if victimBufPage.isDirty {
		heapFile, err := bp.heapFile(victimBufPage.PageId.FileId)
		if err != nil {
			return nil, err
		}

		err = heapFile.Write(victimBufPage.PageId.PageNumber, victimBufPage.Page.ToBytes())
		if err != nil {
			return nil, err
		}

		bp.flushList.delete(victimBufPage.PageId)
	}

	// 新しいページに置き換え
	bp.pageTable.update(victimBufPage.PageId, pageId, victimBufId)
	newBufPage, err := newPage(pageId)
	if err != nil {
		return nil, err
	}
	bp.pages[victimBufId] = *newBufPage
	bp.lru.access(victimBufId)
	return &bp.pages[victimBufId], nil
}

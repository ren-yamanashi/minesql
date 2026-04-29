package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// AddPage はバッファプールに新しいページを追加する
//
// バッファプールに空きがある場合は新しいページを追加し、空きがない場合は古いページを新しいページに置き換える
func (bp *BufferPool) AddPage(pageId page.PageId) (*BufferPage, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	return bp.addPage(pageId)
}

// addPage はバッファプールに新しいページを追加する
func (bp *BufferPool) addPage(pageId page.PageId) (*BufferPage, error) {
	// バッファプールに空きがある場合: 新しいバッファページを追加・ページテーブルを更新
	if len(bp.bufferPages) < int(bp.MaxNumOfPage) {
		newBufPage, err := NewBufferPage(pageId)
		if err != nil {
			return nil, err
		}
		bp.bufferPages = append(bp.bufferPages, *newBufPage)
		bufferId := BufferId(len(bp.bufferPages) - 1)
		bp.pageTable.Add(pageId, bufferId)
		bp.lru.Access(bufferId)
		return &bp.bufferPages[bufferId], nil
	}

	// バッファプールに空きがない場合: ページを追い出す
	victimBufId := bp.lru.Evict()
	victimBufPage := &bp.bufferPages[victimBufId]

	if victimBufPage.IsDirty {
		heapFile, err := bp.getHeapFile(victimBufPage.PageId.FileId)
		if err != nil {
			return nil, err
		}

		err = heapFile.Write(victimBufPage.PageId.PageNumber, victimBufPage.Page.ToBytes())
		if err != nil {
			return nil, err
		}

		bp.flushList.Remove(victimBufPage.PageId)
	}

	// 新しいページに置き換え
	bp.pageTable.Update(victimBufPage.PageId, pageId, victimBufId)
	newBufPage, err := NewBufferPage(pageId)
	if err != nil {
		return nil, err
	}
	bp.bufferPages[victimBufId] = *newBufPage
	bp.lru.Access(victimBufId)
	return &bp.bufferPages[victimBufId], nil
}

package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// GetWritePage は書き込み用のページデータを取得する
func (bp *BufferPool) GetWritePage(pageId page.Id) (*page.Page, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bufPage, err := bp.fetchPage(pageId)
	if err != nil {
		return nil, err
	}

	// 書き込み用なのでダーティーページとして扱う
	if !bufPage.isDirty {
		bufPage.isDirty = true
		bp.flushList.add(pageId)
	}
	return bufPage.Page, nil
}

// GetReadPage は読み込み用のページデータを取得する
func (bp *BufferPool) GetReadPage(pageId page.Id) (*page.Page, error) {
	// ページがバッファプールにある場合は RLock で返す (LRU 更新不要な為)
	bp.mutex.RLock()
	if bufId, exists := bp.pageTable.getBufferId(pageId); exists {
		bufPage := &bp.bufferPages[bufId]
		bp.mutex.RUnlock()
		return bufPage.Page, nil
	}
	bp.mutex.RUnlock()

	// ページテーブルにない場合はディスクから読み込む
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bufPage, err := bp.fetchPage(pageId)
	if err != nil {
		return nil, err
	}
	return bufPage.Page, nil
}

// FetchPage は指定された pageId のバッファページを取得する
func (bp *BufferPool) FetchPage(pageId page.Id) (*BufferPage, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	return bp.fetchPage(pageId)
}

// IsPageCached は指定ページがバッファプールに載っているかを返す
func (bp *BufferPool) IsPageCached(pageId page.Id) bool {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	_, ok := bp.pageTable.getBufferId(pageId)
	return ok
}

// UnRefPage は指定されたページの参照を解除し、優先的に追い出されるようにする
func (bp *BufferPool) UnRefPage(pageId page.Id) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	if bufferId, exists := bp.pageTable.getBufferId(pageId); exists {
		bp.lru.Delete(bufferId)
	}
}

// fetchPage は指定されたページをバッファプールから取得する
func (bp *BufferPool) fetchPage(pageId page.Id) (*BufferPage, error) {
	// ページがバッファプールにある場合
	if bufferId, exists := bp.pageTable.getBufferId(pageId); exists {
		bufferPage := &bp.bufferPages[bufferId]
		bp.lru.access(bufferId)
		return bufferPage, nil
	}

	// ページがバッファプールにない場合
	bufPage, err := bp.addPage(pageId)
	if err != nil {
		return nil, err
	}

	// ディスク上のファイルからページを読み込む
	heapFile, err := bp.getHeapFile(pageId.FileId)
	if err != nil {
		return nil, err
	}
	err = heapFile.Read(pageId.PageNumber, bufPage.Page.ToBytes())
	if err != nil {
		return nil, err
	}
	bufPage.PageId = pageId
	bufPage.isDirty = false

	return bufPage, nil
}

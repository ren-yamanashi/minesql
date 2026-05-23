package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// BufferPageForWrite は書き込み用のページデータを取得する
func (bp *Pool) BufferPageForWrite(pageId page.Id) (*page.Page, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bufPage, err := bp.bufferPage(pageId)
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

// BufferPageForRead は読み込み用のページデータを取得する
func (bp *Pool) BufferPageForRead(pageId page.Id) (*page.Page, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bufPage, err := bp.bufferPage(pageId)
	if err != nil {
		return nil, err
	}
	return bufPage.Page, nil
}

// BufferPage は指定された pageId のバッファページを取得する
func (bp *Pool) BufferPage(pageId page.Id) (*Page, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	return bp.bufferPage(pageId)
}

// IsPageCached は指定ページがバッファプールに載っているかを返す
func (bp *Pool) IsPageCached(pageId page.Id) bool {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	_, ok := bp.pageTable.bufferId(pageId)
	return ok
}

// UnRefPage は指定されたページの参照を解除し、優先的に追い出されるようにする
func (bp *Pool) UnRefPage(pageId page.Id) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	if bufferId, exists := bp.pageTable.bufferId(pageId); exists {
		bp.lru.Delete(bufferId)
	}
}

// bufferPage は指定されたページをバッファプールから取得する
func (bp *Pool) bufferPage(pageId page.Id) (*Page, error) {
	// ページがバッファプールにある場合
	if bufferId, exists := bp.pageTable.bufferId(pageId); exists {
		bufferPage := &bp.pages[bufferId]
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

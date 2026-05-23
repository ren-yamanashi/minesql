package buffer

import (
	"fmt"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type Pool struct {
	MaxNumOfPage int // バッファプールの最大バッファページ数
	flushList    *flushList
	mutex        sync.RWMutex
	files        map[page.FileId]*file.HeapFile
	pages        []Page
	pageTable    pageTable
	lru          *lru
}

func NewPool(size int) *Pool {
	var maxNumOfPage int
	if size <= page.PageSize {
		maxNumOfPage = 1
	} else {
		maxNumOfPage = (size / page.PageSize) + 1
	}
	return &Pool{
		flushList:    newFlushList(),
		MaxNumOfPage: maxNumOfPage,
		files:        make(map[page.FileId]*file.HeapFile),
		pages:        make([]Page, 0, maxNumOfPage),
		pageTable:    newPageTable(),
		lru:          newLru(maxNumOfPage),
	}
}

// BufferPageForWrite は書き込み用のバッファページを取得する
func (bp *Pool) BufferPageForWrite(pageId page.Id) (*Page, error) {
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
	return bufPage, nil
}

// BufferPageForRead は読み込み用のバッファページを取得する
func (bp *Pool) BufferPageForRead(pageId page.Id) (*Page, error) {
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

// AllocatePageId は指定された FileId に対して新しい PageId を割り当てる
func (bp *Pool) AllocatePageId(fileId page.FileId) (page.Id, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	heapFile, err := bp.heapFile(fileId)
	if err != nil {
		return page.InvalidId, err
	}
	return heapFile.AllocatePageId(), nil
}

// RegisterHeapFile は BufferPool に HeapFile を登録する
//   - fileId: 登録する HeapFile に対応する FileId
//   - heapFile: 登録する HeapFile
func (bp *Pool) RegisterHeapFile(fileId page.FileId, heapFile *file.HeapFile) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	bp.files[fileId] = heapFile
}

// HeapFile は指定された FileId に対応する HeapFile を取得する
func (bp *Pool) HeapFile(fileId page.FileId) (*file.HeapFile, error) {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	return bp.heapFile(fileId)
}

// heapFile は指定された FileId に対応する HeapFile を取得する
func (bp *Pool) heapFile(fileId page.FileId) (*file.HeapFile, error) {
	heapFile, ok := bp.files[fileId]
	if !ok {
		return nil, fmt.Errorf("heap file for FileId %d not found", fileId)
	}
	return heapFile, nil
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
	heapFile, err := bp.heapFile(pageId.FileId)
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

package buffer

import (
	"fmt"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type BufferId uint64

type BufferPool struct {
	MaxNumOfPage int // バッファプールの最大バッファページ数
	flushList    *flushList
	mutex        sync.RWMutex
	files        map[page.FileId]*file.HeapFile
	bufferPages  []BufferPage
	pageTable    pageTable
	lru          *lru
}

func NewBufferPool(size int) *BufferPool {
	var maxNumOfPage int
	if size <= page.PageSize {
		maxNumOfPage = 1
	} else {
		maxNumOfPage = (size / page.PageSize) + 1
	}
	return &BufferPool{
		flushList:    newFlushList(),
		MaxNumOfPage: maxNumOfPage,
		files:        make(map[page.FileId]*file.HeapFile),
		bufferPages:  make([]BufferPage, 0, maxNumOfPage),
		pageTable:    newPageTable(),
		lru:          newLru(maxNumOfPage),
	}
}

// AllocatePageId は指定された FileId に対して新しい PageId を割り当てる
func (bp *BufferPool) AllocatePageId(fileId page.FileId) (page.Id, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	heapFile, err := bp.getHeapFile(fileId)
	if err != nil {
		return page.InvalidId, err
	}
	return heapFile.AllocatePageId(), nil
}

// RegisterHeapFile は BufferPool に HeapFile を登録する
//   - fileId: 登録する HeapFile に対応する FileId
//   - heapFile: 登録する HeapFile
func (bp *BufferPool) RegisterHeapFile(fileId page.FileId, heapFile *file.HeapFile) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	bp.files[fileId] = heapFile
}

// BUfferId は指定された pageId に対応するバッファページを取得する
func (bp *BufferPool) BufferPage(pageId page.Id) (*BufferPage, bool) {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	bufferId, ok := bp.pageTable[pageId]
	if !ok {
		return nil, false
	}
	return &bp.bufferPages[bufferId], true
}

// HeapFile は指定された FileId に対応する HeapFile を取得する
func (bp *BufferPool) HeapFile(fileId page.FileId) (*file.HeapFile, error) {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	return bp.getHeapFile(fileId)
}

// getHeapFile は指定された FileId に対応する HeapFile を取得する
func (bp *BufferPool) getHeapFile(fileId page.FileId) (*file.HeapFile, error) {
	heapFile, ok := bp.files[fileId]
	if !ok {
		return nil, fmt.Errorf("heap file for FileId %d not found", fileId)
	}
	return heapFile, nil
}

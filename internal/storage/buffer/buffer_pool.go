package buffer

import (
	"fmt"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

type BufferId uint64

type BufferPool struct {
	mutex        sync.RWMutex
	files        map[page.FileId]*file.HeapFile
	bufferPages  []BufferPage
	pageTable    PageTable
	lru          *LRU
	flushList    *FlushList
	MaxNumOfPage uint32 // バッファプールの最大バッファページ数
}

func NewBufferPool(size uint32) *BufferPool {
	var maxNumOfPage uint32
	if size <= page.PageSize {
		maxNumOfPage = uint32(1)
	} else {
		maxNumOfPage = (size / page.PageSize) + 1
	}
	return &BufferPool{
		files:        make(map[page.FileId]*file.HeapFile),
		bufferPages:  make([]BufferPage, 0, maxNumOfPage),
		MaxNumOfPage: maxNumOfPage,
		pageTable:    NewPageTable(),
		lru:          NewLRU(int(maxNumOfPage)),
		flushList:    NewFlushList(),
	}
}

// AllocatePageId は指定された FileId に対して新しい PageId を割り当てる
func (bp *BufferPool) AllocatePageId(fileId page.FileId) (page.PageId, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	heapFile, err := bp.getHeapFile(fileId)
	if err != nil {
		return page.InvalidPageId, err
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

// GetHeapFile は指定された FileId に対応する HeapFile を取得する
func (bp *BufferPool) GetHeapFile(fileId page.FileId) (*file.HeapFile, error) {
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

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

// AllocatePageId は指定された FileId に対して新しい PageId を割り当てる
func (bp *Pool) AllocatePageId(fileId page.FileId) (page.Id, error) {
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
func (bp *Pool) RegisterHeapFile(fileId page.FileId, heapFile *file.HeapFile) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	bp.files[fileId] = heapFile
}

// HeapFile は指定された FileId に対応する HeapFile を取得する
func (bp *Pool) HeapFile(fileId page.FileId) (*file.HeapFile, error) {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	return bp.getHeapFile(fileId)
}

// getHeapFile は指定された FileId に対応する HeapFile を取得する
func (bp *Pool) getHeapFile(fileId page.FileId) (*file.HeapFile, error) {
	heapFile, ok := bp.files[fileId]
	if !ok {
		return nil, fmt.Errorf("heap file for FileId %d not found", fileId)
	}
	return heapFile, nil
}

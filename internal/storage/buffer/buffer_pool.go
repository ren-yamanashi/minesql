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
	newlyDirtied []page.PageId // 前回の PopNewlyDirtied 以降にダーティーになったページ
	MaxNumOfPage uint32        // バッファプールの最大バッファページ数
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

// GetWritePageData は書き込み用のページデータを取得する
func (bp *BufferPool) GetWritePageData(pageId page.PageId) (*page.Page, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bufPage, err := bp.fetchPage(pageId)
	if err != nil {
		return nil, err
	}

	// 書き込み用なのでダーティーページとして扱う
	if !bufPage.IsDirty {
		bufPage.IsDirty = true
		bp.flushList.Add(pageId)
	}
	bp.newlyDirtied = append(bp.newlyDirtied, pageId)
	return bufPage.Page, nil
}

// GetReadPageData は読み込み用のページデータを取得する
func (bp *BufferPool) GetReadPageData(pageId page.PageId) (*page.Page, error) {
	// ページがバッファプールにある場合は RLock で返す (LRU 更新不要な為)
	bp.mutex.RLock()
	if bufId, exists := bp.pageTable.GetBufferId(pageId); exists {
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
func (bp *BufferPool) FetchPage(pageId page.PageId) (*BufferPage, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	return bp.fetchPage(pageId)
}

// AddPage はバッファプールに新しいページを追加する
//
// バッファプールに空きがある場合は新しいページを追加し、空きがない場合は古いページを新しいページに置き換える
func (bp *BufferPool) AddPage(pageId page.PageId) (*BufferPage, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	return bp.addPage(pageId)
}

// IsPageCached は指定ページがバッファプールに載っているかを返す
func (bp *BufferPool) IsPageCached(pageId page.PageId) bool {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	_, ok := bp.pageTable.GetBufferId(pageId)
	return ok
}

// UnRefPage は指定されたページの参照を解除し、優先的に追い出されるようにする
func (bp *BufferPool) UnRefPage(pageId page.PageId) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	if bufferId, exists := bp.pageTable.GetBufferId(pageId); exists {
		bp.lru.Remove(bufferId)
	}
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

// fetchPage は指定されたページをバッファプールから取得する
func (bp *BufferPool) fetchPage(pageId page.PageId) (*BufferPage, error) {
	// ページがバッファプールにある場合
	if bufferId, exists := bp.pageTable.GetBufferId(pageId); exists {
		bufferPage := &bp.bufferPages[bufferId]
		bp.lru.Access(bufferId)
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
	bufPage.IsDirty = false

	return bufPage, nil
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

// getHeapFile は指定された FileId に対応する HeapFile を取得する
func (bp *BufferPool) getHeapFile(fileId page.FileId) (*file.HeapFile, error) {
	disk, ok := bp.files[fileId]
	if !ok {
		return nil, fmt.Errorf("disk for FileId %d not found", fileId)
	}
	return disk, nil
}

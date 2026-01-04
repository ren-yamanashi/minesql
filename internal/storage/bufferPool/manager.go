package bufferpool

import (
	"minesql/internal/storage/disk"
)

type BufferPoolManager struct {
	diskManager *disk.DiskManager
	bufferPool  BufferPool
	pageTable   PageTable
}

func NewBufferPoolManager(dm *disk.DiskManager, size int) *BufferPoolManager {
	return &BufferPoolManager{
		diskManager: dm,
		bufferPool:  *NewBufferPool(size),
		pageTable:   make(PageTable),
	}
}

// 指定されたページIDのページをバッファプールから取得
// ページがバッファプールに存在しない場合は、ディスクから読み込む
func (bpm *BufferPoolManager) FetchPage(pageId disk.PageId) (*BufferPage, error) {
	// ページテーブルにページがすでにある場合
	if bufferId, ok := bpm.pageTable[pageId]; ok {
		bufferPage := &bpm.bufferPool.BufferPages[bufferId]
		bufferPage.Referenced = true
		return bufferPage, nil
	}

	// ページがバッファプールにない場合
	bufferPage, err := bpm.AddPage(pageId)
	if err != nil {
		return nil, err
	}

	// ディスクからページを読み込む
	err = bpm.diskManager.ReadPageData(pageId, bufferPage.Page[:])
	if err != nil {
		return nil, err
	}
	bufferPage.PageId = pageId
	bufferPage.Referenced = true
	bufferPage.IsDirty = false

	return bufferPage, nil
}

// バッファプールに新しいページを追加
// バッファプールに空きがある場合は新しいページを追加し、空きがない場合は古いページを新しいページに置き換える
func (bpm *BufferPoolManager) AddPage(pageId disk.PageId) (*BufferPage, error) {
	// バッファに空きがある場合
	if len(bpm.bufferPool.BufferPages) < bpm.bufferPool.MaxBufferSize {
		bpm.bufferPool.BufferPages = append(bpm.bufferPool.BufferPages, *NewBufferPage(pageId))
		bufferId := BufferId(len(bpm.bufferPool.BufferPages) - 1)
		bpm.addPageToTable(pageId, bufferId)
		return &bpm.bufferPool.BufferPages[bufferId], nil
	}

	// バッファに空きがない場合
	bufferPageForEvict := bpm.bufferPool.EvictPage()
	if bufferPageForEvict.IsDirty {
		err := bpm.diskManager.WritePageData(bufferPageForEvict.PageId, bufferPageForEvict.Page[:])
		if err != nil {
			return nil, err
		}
		bufferPageForEvict.IsDirty = false
	}
	bpm.updatePageTable(bufferPageForEvict.PageId, pageId, bpm.bufferPool.Pointer)

	// 新しいページに置き換え
	bpm.bufferPool.BufferPages[bpm.bufferPool.Pointer] = *NewBufferPage(pageId)
	newBufferPage := &bpm.bufferPool.BufferPages[bpm.bufferPool.Pointer]
	bpm.bufferPool.AdvancePointer()
	return newBufferPage, nil
}

// ページテーブルを更新
// evictPageId で指定したページが現在のバッファに属している場合のみ削除
func (bpm *BufferPoolManager) updatePageTable(evictPageId disk.PageId, newPageId disk.PageId, bufferId BufferId) {
	if oldBufferId, ok := bpm.pageTable[evictPageId]; ok && oldBufferId == bufferId {
		delete(bpm.pageTable, evictPageId)
	}
	bpm.pageTable[newPageId] = bufferId
}

// ページテーブルに新しいエントリを追加
func (bpm *BufferPoolManager) addPageToTable(pageId disk.PageId, bufferId BufferId) {
	bpm.pageTable[pageId] = bufferId
}

// バッファプールを取得 (動作確認用)
func (bpm *BufferPoolManager) GetBufferPool() *BufferPool {
	return &bpm.bufferPool
}

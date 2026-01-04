package bufferPool

import (
	"minesql/internal/storage/disk"
)

type BufferPoolManager struct {
	diskManager *disk.DiskManager
	bufferPool  BufferPool
	pageTable   map[disk.PageId]Pointer
}

func NewBufferPoolManager(dm *disk.DiskManager, size int) *BufferPoolManager {
	return &BufferPoolManager{
		diskManager: dm,
		bufferPool:  *NewBufferPool(size),
		pageTable:   make(map[disk.PageId]Pointer),
	}
}

// 指定されたページIDのページをバッファプールから取得
// ページがバッファプールに存在しない場合は、ディスクから読み込む
func (bpm *BufferPoolManager) FetchPage(pageId disk.PageId) (*BufferPage, error) {
	// ページテーブルにページがすでにある場合
	if frameId, ok := bpm.pageTable[pageId]; ok {
		frame := &bpm.bufferPool.BufferPages[frameId]
		frame.Referenced = true
		return frame, nil
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
		bpm.addPageToTable(pageId, bpm.bufferPool.Pointer)
		return &bpm.bufferPool.BufferPages[bpm.bufferPool.Pointer], nil
	}

	// バッファが空きがない場合
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
	newBufferPages := &bpm.bufferPool.BufferPages[bpm.bufferPool.Pointer]
	bpm.bufferPool.AdvancePointer()
	return newBufferPages, nil
}

// ページテーブルを更新
// evictPageId で指定したページが現在のバッファに属している場合のみ削除
func (bpm *BufferPoolManager) updatePageTable(evictPageId disk.PageId, newPageId disk.PageId, pointer Pointer) {
	if oldBufferId, ok := bpm.pageTable[evictPageId]; ok && oldBufferId == pointer {
		delete(bpm.pageTable, evictPageId)
	}
	bpm.pageTable[newPageId] = pointer
}

// ページテーブルに新しいエントリを追加
func (bpm *BufferPoolManager) addPageToTable(pageId disk.PageId, pointer Pointer) {
	bpm.pageTable[pageId] = pointer
}

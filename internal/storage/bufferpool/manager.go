package bufferpool

import (
	"minesql/internal/storage/disk"
)

type BufferPoolManager struct {
	DiskManager disk.DiskManagerInterface
	bufpool     BufferPool
	pageTable   PageTable
}

func NewBufferPoolManager(dm disk.DiskManagerInterface, size int) *BufferPoolManager {
	return &BufferPoolManager{
		DiskManager: dm,
		bufpool:     *NewBufferPool(size),
		pageTable:   make(PageTable),
	}
}

// 指定されたページIDのページをバッファプールから取得
// ページがバッファプールに存在しない場合は、ディスクから読み込む
func (bpm *BufferPoolManager) FetchPage(pageId disk.PageId) (*BufferPage, error) {
	// ページテーブルにページがすでにある場合
	if bufferId, ok := bpm.pageTable[pageId]; ok {
		bufferPage := &bpm.bufpool.BufferPages[bufferId]
		bufferPage.Referenced = true
		return bufferPage, nil
	}

	// ページがバッファプールにない場合
	bufferPage, err := bpm.AddPage(pageId)
	if err != nil {
		return nil, err
	}

	// ディスクからページを読み込む
	err = bpm.DiskManager.ReadPageData(pageId, bufferPage.Page[:])
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
	if len(bpm.bufpool.BufferPages) < bpm.bufpool.MaxBufferSize {
		bpm.bufpool.BufferPages = append(bpm.bufpool.BufferPages, *NewBufferPage(pageId))
		bufferId := BufferId(len(bpm.bufpool.BufferPages) - 1)
		bpm.addPageToTable(pageId, bufferId)
		return &bpm.bufpool.BufferPages[bufferId], nil
	}

	// バッファに空きがない場合
	bufferPageForEvict := bpm.bufpool.EvictPage()
	if bufferPageForEvict.IsDirty {
		err := bpm.DiskManager.WritePageData(bufferPageForEvict.PageId, bufferPageForEvict.Page[:])
		if err != nil {
			return nil, err
		}
		bufferPageForEvict.IsDirty = false
	}
	bpm.updatePageTable(bufferPageForEvict.PageId, pageId, bpm.bufpool.Pointer)

	// 新しいページに置き換え
	bpm.bufpool.BufferPages[bpm.bufpool.Pointer] = *NewBufferPage(pageId)
	newBufferPage := &bpm.bufpool.BufferPages[bpm.bufpool.Pointer]
	bpm.bufpool.AdvancePointer()
	return newBufferPage, nil
}

// 指定されたページの参照ビットをクリア
func (bpm *BufferPoolManager) UnRefPage(pageId disk.PageId) {
	if bufferId, ok := bpm.pageTable[pageId]; ok {
		bpm.bufpool.BufferPages[bufferId].Referenced = false
	}
}

// バッファプール内のすべてのダーティーページをディスクに書き出す
func (bpm *BufferPoolManager) FlushPage() error {
	for pageId, bufferId := range bpm.pageTable {
		bufferPage := &bpm.bufpool.BufferPages[bufferId]
		if !bufferPage.IsDirty {
			continue
		}
		err := bpm.DiskManager.WritePageData(pageId, bufferPage.Page[:])
		if err != nil {
			return err
		}
		bufferPage.IsDirty = false
	}
	return bpm.DiskManager.Sync()
}

// 指定されたページ ID のバッファページを取得する
func (bpm *BufferPoolManager) GetBufferPage(pageId disk.PageId) (*BufferPage, bool) {
	if bufferId, ok := bpm.pageTable[pageId]; ok {
		return &bpm.bufpool.BufferPages[bufferId], true
	}
	return nil, false
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

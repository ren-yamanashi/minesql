package bufferpool

import (
	"fmt"
	"minesql/internal/storage/disk"
	"minesql/internal/storage/page"
)

type BufferPoolManager struct {
	diskManagers  map[page.FileId]*disk.DiskManager // FileId → DiskManager のマップ
	bufpool       BufferPool
	pageTable     pageTable
	nextFileId    page.FileId // 次に割り当てる FileId
	baseDirectory string      // ディスクファイルの基本ディレクトリ
}

func NewBufferPoolManager(size int, baseDirectory string) *BufferPoolManager {
	return &BufferPoolManager{
		diskManagers:  make(map[page.FileId]*disk.DiskManager),
		bufpool:       *newBufferPool(size),
		pageTable:     make(pageTable),
		nextFileId:    page.FileId(1), // FileId 1 から開始
		baseDirectory: baseDirectory,
	}
}

// DiskManager を登録する
func (bpm *BufferPoolManager) RegisterDiskManager(fileId page.FileId, dm *disk.DiskManager) {
	bpm.diskManagers[fileId] = dm
}

// DiskManager を取得する
func (bpm *BufferPoolManager) GetDiskManager(fileId page.FileId) (*disk.DiskManager, error) {
	dm, ok := bpm.diskManagers[fileId]
	if !ok {
		return nil, fmt.Errorf("DiskManager for FileId %d not found", fileId)
	}
	return dm, nil
}

// 新しい FileId を割り当てる
func (bpm *BufferPoolManager) AllocateFileId() page.FileId {
	fileId := bpm.nextFileId
	bpm.nextFileId++
	return fileId
}

// 指定された FileId に対して新しい PageId を割り当てる
func (bpm *BufferPoolManager) AllocatePageId(fileId page.FileId) (page.PageId, error) {
	dm, err := bpm.GetDiskManager(fileId)
	if err != nil {
		return page.INVALID_PAGE_ID, err
	}
	return dm.AllocatePage(), nil
}

// 指定されたページIDのページをバッファプールから取得
// ページがバッファプールに存在しない場合は、ディスクから読み込む
func (bpm *BufferPoolManager) FetchPage(pageId page.PageId) (*BufferPage, error) {
	// FileId から DiskManager を取得
	dm, err := bpm.GetDiskManager(pageId.FileId)
	if err != nil {
		return nil, err
	}

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
	err = dm.ReadPageData(pageId, bufferPage.Page[:])
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
func (bpm *BufferPoolManager) AddPage(pageId page.PageId) (*BufferPage, error) {
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
		// FileId から DiskManager を取得
		dm, err := bpm.GetDiskManager(bufferPageForEvict.PageId.FileId)
		if err != nil {
			return nil, err
		}
		// ダーティーページをディスクに書き出す
		err = dm.WritePageData(bufferPageForEvict.PageId, bufferPageForEvict.Page[:])
		if err != nil {
			return nil, err
		}
		bufferPageForEvict.IsDirty = false
	}

	// ページテーブルを更新 (追い出すページを削除し、新しいページを追加)
	bpm.updatePageTable(bufferPageForEvict.PageId, pageId, bpm.bufpool.Pointer)

	// 新しいページに置き換え
	bpm.bufpool.BufferPages[bpm.bufpool.Pointer] = *NewBufferPage(pageId)
	newBufPage := &bpm.bufpool.BufferPages[bpm.bufpool.Pointer]
	bpm.bufpool.AdvancePointer()
	return newBufPage, nil
}

// 指定されたページの参照ビットをクリア
func (bpm *BufferPoolManager) UnRefPage(pageId page.PageId) {
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

		// FileId から DiskManager を取得
		dm, err := bpm.GetDiskManager(pageId.FileId)
		if err != nil {
			return err
		}

		// ダーティーページをディスクに書き出す
		err = dm.WritePageData(pageId, bufferPage.Page[:])
		if err != nil {
			return err
		}
		bufferPage.IsDirty = false
	}

	// すべての DiskManager を Sync
	for _, dm := range bpm.diskManagers {
		if err := dm.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// ページテーブルを更新
// evictPageId で指定したページが現在のバッファに属している場合のみ削除
func (bpm *BufferPoolManager) updatePageTable(evictPageId page.PageId, newPageId page.PageId, bufferId BufferId) {
	if oldBufferId, ok := bpm.pageTable[evictPageId]; ok && oldBufferId == bufferId {
		delete(bpm.pageTable, evictPageId)
	}
	bpm.pageTable[newPageId] = bufferId
}

// ページテーブルに新しいエントリを追加
func (bpm *BufferPoolManager) addPageToTable(pageId page.PageId, bufferId BufferId) {
	bpm.pageTable[pageId] = bufferId
}

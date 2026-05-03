package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// FlushAllPages はバッファプール内のすべてのダーティーページをフラッシュする
func (bp *BufferPool) FlushAllPages() error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	var flushErr error

	// 全ダーティーページをディスクに書き出す
	bp.pageTable.ForEach(func(pageId page.PageId, bufId BufferId) {
		if flushErr != nil {
			return
		}

		bufPage := &bp.bufferPages[bufId]
		if !bufPage.isDirty {
			return
		}

		heapFile, err := bp.getHeapFile(pageId.FileId)
		if err != nil {
			flushErr = err
			return
		}

		err = heapFile.Write(pageId.PageNumber, bufPage.Page.ToBytes())
		if err != nil {
			flushErr = err
			return
		}
		bufPage.isDirty = false
	})
	if flushErr != nil {
		return flushErr
	}

	bp.flushList.Clear()

	for _, hf := range bp.files {
		if err := hf.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// FlushOldestPages はフラッシュリストの先頭から n ページをディスクにフラッシュする
func (bp *BufferPool) FlushOldestPages(n int) error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	pageIds := bp.flushList.OldestPageIds(n)
	if len(pageIds) == 0 {
		return nil
	}

	// フラッシュ対象のディスクを記録する (後でまとめて Sync するため)
	syncHeapFiles := make(map[page.FileId]bool)

	// 対象のダーティーページをディスクに書き出す
	for _, pid := range pageIds {
		bufId, exists := bp.pageTable.GetBufferId(pid)
		if !exists {
			continue
		}

		bufPage := &bp.bufferPages[bufId]
		if !bufPage.isDirty {
			bp.flushList.Delete(pid)
			continue
		}

		heapFile, err := bp.getHeapFile(pid.FileId)
		if err != nil {
			return err
		}
		if err := heapFile.Write(pid.PageNumber, bufPage.Page.ToBytes()); err != nil {
			return err
		}

		bufPage.isDirty = false
		bp.flushList.Delete(pid)
		syncHeapFiles[pid.FileId] = true
	}

	for fileId := range syncHeapFiles {
		heapFile, err := bp.getHeapFile(fileId)
		if err != nil {
			return err
		}
		if err := heapFile.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// FlushListSize はフラッシュリスト内のページ数を返す
func (bp *BufferPool) FlushListSize() uint32 {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	return bp.flushList.numOfPage
}

package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// FlushAllPages はバッファプール内のすべてのダーティーページをフラッシュする
func (bp *Pool) FlushAllPages() error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	var flushErr error

	// 全ダーティーページをディスクに書き出す
	bp.pageTable.forEach(func(pageId page.Id, bufId id) {
		if flushErr != nil {
			return
		}

		bufPage := &bp.pages[bufId]
		if !bufPage.isDirty {
			return
		}

		heapFile, err := bp.heapFile(pageId.FileId)
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

	bp.flushList.clear()

	for _, hf := range bp.files {
		if err := hf.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// FlushOldestPages はフラッシュリストの先頭から n ページをディスクにフラッシュする
func (bp *Pool) FlushOldestPages(n int) error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	pageIds := bp.flushList.oldestPageIds(n)
	if len(pageIds) == 0 {
		return nil
	}

	// フラッシュ対象のディスクを記録する (後でまとめて Sync するため)
	syncHeapFiles := make(map[page.FileId]bool)

	// 対象のダーティーページをディスクに書き出す
	for _, pid := range pageIds {
		bufId, exists := bp.pageTable.bufferId(pid)
		if !exists {
			continue
		}

		bufPage := &bp.pages[bufId]
		if !bufPage.isDirty {
			bp.flushList.delete(pid)
			continue
		}

		heapFile, err := bp.heapFile(pid.FileId)
		if err != nil {
			return err
		}
		if err := heapFile.Write(pid.PageNumber, bufPage.Page.ToBytes()); err != nil {
			return err
		}

		bufPage.isDirty = false
		bp.flushList.delete(pid)
		syncHeapFiles[pid.FileId] = true
	}

	for fileId := range syncHeapFiles {
		heapFile, err := bp.heapFile(fileId)
		if err != nil {
			return err
		}
		if err := heapFile.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// NumOfFlushListPage はフラッシュリスト内のページ数を返す
func (bp *Pool) NumOfFlushListPage() int {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	return bp.flushList.NumOfPage
}

// ForEachDirtyPage はフラッシュリスト内の全ダーティーページに対してコールバックを実行する
func (bp *Pool) ForEachDirtyPage(fn func(pg *page.Page)) {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()

	for node := bp.flushList.Head; node != nil; node = node.next {
		bufId, ok := bp.pageTable.bufferId(node.pageId)
		if !ok {
			continue
		}
		fn(bp.pages[bufId].Page)
	}
}

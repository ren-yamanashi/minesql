package buffer

import "minesql/internal/storage/page"

// FlushAllPages はバッファプール内のすべてのダーティーページをディスクに書き出す
func (bp *BufferPool) FlushAllPages() error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	// REDO ログを先にフラッシュ
	if bp.redoLog != nil {
		if err := bp.redoLog.Flush(); err != nil {
			return err
		}
	}

	// 全ダーティーページをディスクに書き出す
	for pageId, bufferId := range bp.pageTable {
		bufferPage := &bp.bufferPages[bufferId]
		if !bufferPage.IsDirty {
			continue
		}

		// FileId から Disk を取得
		disk, err := bp.getDisk(pageId.FileId)
		if err != nil {
			return err
		}

		// ダーティーページをディスクに書き出す
		err = disk.WritePageData(pageId, bufferPage.Page)
		if err != nil {
			return err
		}
		bufferPage.IsDirty = false
	}

	// フラッシュリストをクリア
	bp.flushList.Clear()

	// 全ディスクを Sync してストレージデバイスへの書き込みを保証
	for _, disk := range bp.disks {
		if err := disk.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// FlushOldestPages はフラッシュリストの先頭から n ページをディスクにフラッシュする
func (bp *BufferPool) FlushOldestPages(n int) error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	// フラッシュリストの先頭 (最も古いダーティーページ) から n 件取得
	pageIds := bp.flushList.OldestPageIds(n)
	if len(pageIds) == 0 {
		return nil
	}

	// ダーティーページをディスクに書き出す前に、REDO ログバッファを先にフラッシュする
	if bp.redoLog != nil {
		if err := bp.redoLog.Flush(); err != nil {
			return err
		}
	}

	// フラッシュ対象のディスクを記録する (後でまとめて Sync するため)
	syncDisks := make(map[page.FileId]bool)

	for _, pid := range pageIds {
		// ページテーブルからバッファページを取得
		bufferId, ok := bp.pageTable[pid]
		if !ok {
			continue
		}

		// 既にクリーンなページはフラッシュリストから除外するだけ
		bufferPage := &bp.bufferPages[bufferId]
		if !bufferPage.IsDirty {
			bp.flushList.Remove(pid)
			continue
		}

		// ダーティーページをディスクに書き出す
		disk, err := bp.getDisk(pid.FileId)
		if err != nil {
			return err
		}
		if err := disk.WritePageData(pid, bufferPage.Page); err != nil {
			return err
		}

		// ページをクリーンにし、フラッシュリストから除外
		bufferPage.IsDirty = false
		bp.flushList.Remove(pid)
		syncDisks[pid.FileId] = true
	}

	// フラッシュしたページのディスクを Sync してストレージデバイスへの書き込みを保証
	for fileId := range syncDisks {
		disk, err := bp.getDisk(fileId)
		if err != nil {
			return err
		}
		if err := disk.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// PopNewlyDirtied は前回の呼び出し以降に書き込みが行われたページの PageId を返し、newlyDirtied リストをクリアする
func (bp *BufferPool) PopNewlyDirtied() []page.PageId {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	result := bp.newlyDirtied
	bp.newlyDirtied = nil
	return result
}

// ClearNewlyDirtied は newlyDirtied リストをクリアする
func (bp *BufferPool) ClearNewlyDirtied() {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	bp.newlyDirtied = nil
}

// MinPageLSN はフラッシュリスト内の全ダーティーページの最小 Page LSN を返す
func (bp *BufferPool) MinPageLSN() uint32 {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	return bp.flushList.MinPageLSN(bp.bufferPages, bp.pageTable)
}

// FlushListSize はフラッシュリスト内のページ数を返す
func (bp *BufferPool) FlushListSize() int {
	bp.mutex.RLock()
	defer bp.mutex.RUnlock()
	return bp.flushList.Size
}

package buffer

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// FlushAllPages はバッファプール内のすべてのダーティーページをフラッシュする
func (p *Pool) FlushAllPages() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var flushErr error

	// 全ダーティーページをディスクに書き出す
	p.pageTable.forEach(func(pageId page.Id, bufId id) {
		if flushErr != nil {
			return
		}

		bufPage := &p.pages[bufId]
		if !bufPage.isDirty {
			return
		}

		heapFile, err := p.heapFile(pageId.FileId)
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

	p.flushList.clear()

	for _, hf := range p.files {
		if err := hf.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// FlushOldestPages はフラッシュリストの先頭から n ページをディスクにフラッシュする
func (p *Pool) FlushOldestPages(n int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	pageIds := p.flushList.oldestPageIds(n)
	if len(pageIds) == 0 {
		return nil
	}

	// フラッシュ対象のディスクを記録する (後でまとめて Sync するため)
	syncHeapFiles := make(map[page.FileId]bool)

	// 対象のダーティーページをディスクに書き出す
	for _, pid := range pageIds {
		bufId, exists := p.pageTable.bufferId(pid)
		if !exists {
			continue
		}

		bufPage := &p.pages[bufId]
		if !bufPage.isDirty {
			p.flushList.delete(pid)
			continue
		}

		heapFile, err := p.heapFile(pid.FileId)
		if err != nil {
			return err
		}
		if err := heapFile.Write(pid.PageNumber, bufPage.Page.ToBytes()); err != nil {
			return err
		}

		bufPage.isDirty = false
		p.flushList.delete(pid)
		syncHeapFiles[pid.FileId] = true
	}

	for fileId := range syncHeapFiles {
		heapFile, err := p.heapFile(fileId)
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
func (p *Pool) NumOfFlushListPage() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.flushList.numOfPage
}

// ForEachDirtyPage はフラッシュリスト内の全ダーティーページに対してコールバックを実行する
func (p *Pool) ForEachDirtyPage(fn func(pg *page.Page)) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for node := p.flushList.head; node != nil; node = node.next {
		bufId, ok := p.pageTable.bufferId(node.pageId)
		if !ok {
			continue
		}
		fn(p.pages[bufId].Page)
	}
}

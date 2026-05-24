package buffer

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// FlushAllPages はバッファプール内のすべてのダーティーページをフラッシュする
func (p *Pool) FlushAllPages() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var flushErr error

	// Sync 成功後に状態を更新するため Write 成功したページを一時保持する
	var pendingPages []*Page

	// 全ダーティーページをディスクに書き出す
	p.pageTable.forEach(func(pageId page.Id, bufId id) {
		if flushErr != nil {
			return
		}

		bufPage := &p.pages[bufId]
		if !bufPage.isDirty {
			return
		}

		heapFile, err := p.heapFile(pageId.FileId())
		if err != nil {
			flushErr = err
			return
		}

		err = heapFile.Write(pageId.PageNumber(), bufPage.data.Bytes())
		if err != nil {
			flushErr = err
			return
		}
		pendingPages = append(pendingPages, bufPage)
	})
	if flushErr != nil {
		return flushErr
	}

	// 全 file の Sync を試行してエラーを集約する (途中失敗で残ファイルの fsync がスキップされないように)
	var syncErr error
	for _, hf := range p.files {
		if err := hf.Sync(); err != nil {
			syncErr = errors.Join(syncErr, err)
		}
	}
	if syncErr != nil {
		return syncErr
	}

	for _, bp := range pendingPages {
		bp.isDirty = false
	}
	p.flushList.clear()

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
	filesToSync := make(map[page.FileId]struct{})

	// Sync 成功後に状態を更新するため Write 成功したページを一時保持する
	var pendingPageIds []page.Id
	var pendingBufPages []*Page

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

		heapFile, err := p.heapFile(pid.FileId())
		if err != nil {
			return err
		}
		if err := heapFile.Write(pid.PageNumber(), bufPage.data.Bytes()); err != nil {
			return err
		}

		pendingPageIds = append(pendingPageIds, pid)
		pendingBufPages = append(pendingBufPages, bufPage)
		filesToSync[pid.FileId()] = struct{}{}
	}

	for fileId := range filesToSync {
		heapFile, err := p.heapFile(fileId)
		if err != nil {
			return err
		}
		if err := heapFile.Sync(); err != nil {
			return err
		}
	}

	for i, pid := range pendingPageIds {
		pendingBufPages[i].isDirty = false
		p.flushList.delete(pid)
	}

	return nil
}

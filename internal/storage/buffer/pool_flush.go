package buffer

import (
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// flushTask は 1 ページ分のフラッシュ対象
type flushTask struct {
	pageId   page.Id
	bufPage  *Page
	heapFile *file.HeapFile
}

// FlushAllPages はバッファプール内のすべてのダーティーページをフラッシュする
func (p *Pool) FlushAllPages() error {
	tasks, files, err := p.collectAllFlushTasks()
	if err != nil {
		return err
	}
	return p.runFlush(tasks, files)
}

// FlushOldestPages はフラッシュリストの先頭から n ページをディスクにフラッシュする
func (p *Pool) FlushOldestPages(n int) error {
	tasks, files, err := p.collectOldestFlushTasks(n)
	if err != nil {
		return err
	}
	return p.runFlush(tasks, files)
}

// collectAllFlushTasks は全ダーティーページからフラッシュ対象を集める
func (p *Pool) collectAllFlushTasks() ([]flushTask, []*file.HeapFile, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var tasks []flushTask
	var collectErr error
	p.pageTable.forEach(func(pageId page.Id, bufId id) {
		if collectErr != nil {
			return
		}
		bufPage := &p.pages[bufId]
		if !bufPage.isDirty {
			return
		}
		hf, err := p.heapFile(pageId.FileId())
		if err != nil {
			collectErr = err
			return
		}
		tasks = append(tasks, flushTask{pageId: pageId, bufPage: bufPage, heapFile: hf})
	})
	if collectErr != nil {
		return nil, nil, collectErr
	}

	files := make([]*file.HeapFile, 0, len(p.files))
	for _, hf := range p.files {
		files = append(files, hf)
	}
	return tasks, files, nil
}

// collectOldestFlushTasks はフラッシュリストの先頭から n ページのフラッシュ対象を集める
func (p *Pool) collectOldestFlushTasks(n int) ([]flushTask, []*file.HeapFile, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pageIds := p.flushList.oldestPageIds(n)
	if len(pageIds) == 0 {
		return nil, nil, nil
	}

	var tasks []flushTask
	filesSet := make(map[page.FileId]*file.HeapFile)
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
		hf, err := p.heapFile(pid.FileId())
		if err != nil {
			return nil, nil, err
		}
		tasks = append(tasks, flushTask{pageId: pid, bufPage: bufPage, heapFile: hf})
		filesSet[pid.FileId()] = hf
	}

	files := make([]*file.HeapFile, 0, len(filesSet))
	for _, hf := range filesSet {
		files = append(files, hf)
	}
	return tasks, files, nil
}

// runFlush は収集済みのフラッシュタスクを実行する
//   - 各ページに対し Exclusive ラッチの試取得を行い、他者が保持中のページはスキップ (flushList に残り次回再試行)
//   - これにより同一 goroutine が保持中のページに対する self-deadlock を避けつつ、torn flush を防ぐ
//   - 並行性のさらなる改善 (Shared ラッチで書き込み並行を許容) は段階 3 で導入する
func (p *Pool) runFlush(tasks []flushTask, files []*file.HeapFile) error {
	if len(tasks) == 0 {
		return nil
	}

	var lockedTasks []flushTask
	for _, task := range tasks {
		if task.bufPage.latch.TryLockExclusive() {
			lockedTasks = append(lockedTasks, task)
		}
	}
	if len(lockedTasks) == 0 {
		return nil
	}

	unlockAll := func() {
		for _, t := range lockedTasks {
			t.bufPage.latch.Unlock(LatchExclusive)
		}
	}

	for _, task := range lockedTasks {
		if err := task.heapFile.Write(task.pageId.PageNumber(), task.bufPage.data.Bytes()); err != nil {
			unlockAll()
			return err
		}
	}

	var syncErr error
	for _, hf := range files {
		if err := hf.Sync(); err != nil {
			syncErr = errors.Join(syncErr, err)
		}
	}
	if syncErr != nil {
		unlockAll()
		return syncErr
	}

	// isDirty 解除と latch 解放は同じ p.mu 区間内で行う
	// 解除後の evict はバッファスロットを上書きするため、その間に p.mu 外で latch 解放しようとすると
	// 解放中の latch フィールド読み取りと race する
	p.mu.Lock()
	for _, task := range lockedTasks {
		task.bufPage.isDirty = false
		p.flushList.delete(task.pageId)
	}
	unlockAll()
	p.mu.Unlock()
	return nil
}

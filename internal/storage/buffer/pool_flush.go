package buffer

import (
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// flushTask は 1 ページ分のフラッシュ対象
type flushTask struct {
	pageId      page.Id
	bufPage     *Page
	heapFile    *file.HeapFile
	modifyCount uint64 // 収集時点の値。Sync 後に変化していたら並行書き込み中とみなし isDirty 解除をスキップする
}

// FlushAllPages はバッファプール内のすべてのダーティーページをフラッシュする
func (p *Pool) FlushAllPages() error {
	tasks, err := p.collectAllFlushTasks()
	if err != nil {
		return err
	}
	return p.runFlush(tasks)
}

// FlushOldestPages はフラッシュリストの先頭から n ページをディスクにフラッシュする
func (p *Pool) FlushOldestPages(n int) error {
	tasks, err := p.collectOldestFlushTasks(n)
	if err != nil {
		return err
	}
	return p.runFlush(tasks)
}

// collectAllFlushTasks は全ダーティーページからフラッシュ対象を集める
func (p *Pool) collectAllFlushTasks() ([]flushTask, error) {
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
		if bufPage.pinCount > 0 {
			return
		}
		hf, err := p.heapFile(pageId.FileId())
		if err != nil {
			collectErr = err
			return
		}
		tasks = append(tasks, flushTask{
			pageId:      pageId,
			bufPage:     bufPage,
			heapFile:    hf,
			modifyCount: bufPage.modifyCount,
		})
	})
	if collectErr != nil {
		return nil, collectErr
	}
	return tasks, nil
}

// collectOldestFlushTasks はフラッシュリストの先頭から n ページのフラッシュ対象を集める
func (p *Pool) collectOldestFlushTasks(n int) ([]flushTask, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pageIds := p.flushList.oldestPageIds(n)
	if len(pageIds) == 0 {
		return nil, nil
	}

	var tasks []flushTask
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
		if bufPage.pinCount > 0 {
			// Pin 解放後の次回フラッシュで再評価するため flushList には残す
			continue
		}
		hf, err := p.heapFile(pid.FileId())
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, flushTask{
			pageId:      pid,
			bufPage:     bufPage,
			heapFile:    hf,
			modifyCount: bufPage.modifyCount,
		})
	}
	return tasks, nil
}

// runFlush は収集済みのフラッシュタスクを 1 ページずつ実行する
//   - 他者がページの Exclusive ラッチを保持中の場合は、その解放を待ってから書き出す
//   - エラー時は当該ページ以降を未処理のまま即 return する (次回フラッシュで再試行される)
func (p *Pool) runFlush(tasks []flushTask) error {
	if len(tasks) == 0 {
		return nil
	}
	if err := p.flushRedoBeforeData(); err != nil {
		return err
	}
	for _, task := range tasks {
		task.bufPage.latch.LockShared()
		if err := p.flushAndClean(task); err != nil {
			return err
		}
	}
	return nil
}

// flushSinglePage はフラッシュリストの先頭から走査し、Shared ラッチを即時取得できた
// 最初のダーティーページ 1 つを書き出してクリーン化する
//   - ラッチの解放は待たない。1 つも取得できなければ何もしない
func (p *Pool) flushSinglePage() error {
	tasks, err := p.collectOldestFlushTasks(p.FlushListPageCount())
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	if err := p.flushRedoBeforeData(); err != nil {
		return err
	}
	for _, task := range tasks {
		if !task.bufPage.latch.TryLockShared() {
			continue
		}
		return p.flushAndClean(task)
	}
	return nil
}

// flushRedoBeforeData はデータページ書き出し前に Redo ログをディスクへ flush する (WAL 規律)
func (p *Pool) flushRedoBeforeData() error {
	return p.redoLog.Flush()
}

// flushAndClean は Shared ラッチ取得済みのタスク 1 件を「書き出し → Sync → クリーン化」する
//   - 呼び出し側がページの Shared ラッチを取得済みであることが前提。ラッチは内部で解放する
func (p *Pool) flushAndClean(task flushTask) error {
	err := task.heapFile.Write(task.pageId.PageNumber(), task.bufPage.data.Bytes())
	task.bufPage.latch.Unlock(LatchShared)
	if err != nil {
		return err
	}
	if err := task.heapFile.Sync(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// 収集時点から modifyCount が変化していれば並行書き込みがあったため
	// isDirty・oldestModificationLsn を維持し flushList に残す (次回フラッシュで再度書き出す)
	if task.modifyCount != task.bufPage.modifyCount {
		return nil
	}
	task.bufPage.isDirty = false
	task.bufPage.oldestModificationLsn = 0
	p.flushList.delete(task.pageId)
	return nil
}

package access

import (
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

// PageCleaner はバックグラウンドでのダーティーページのフラッシュを管理する
type PageCleaner struct {
	bufferPool      *buffer.BufferPool
	redoLog         *redo.Buffer
	checkpoint      *Checkpoint
	redoLogMaxSize  int           // Redo ログの最大サイズ
	maxDirtyPagePct int           // ダーティーページ率の上限
	interval        time.Duration // クリーニング間隔
	ticker          *time.Ticker
	done            chan struct{}
	stopped         chan struct{} // goroutine 終了通知用
}

func NewPageCleaner(bp *buffer.BufferPool, redo *redo.Buffer, redoMaxSize int, maxDirtyPct int) *PageCleaner {
	return &PageCleaner{
		bufferPool:      bp,
		redoLog:         redo,
		checkpoint:      NewCheckpoint(bp, redo),
		redoLogMaxSize:  redoMaxSize,
		maxDirtyPagePct: maxDirtyPct,
		interval:        1 * time.Second,
	}
}

// Start はバックグラウンド goroutine を起動する
func (pc *PageCleaner) Start() {
	pc.ticker = time.NewTicker(pc.interval)
	pc.done = make(chan struct{})
	pc.stopped = make(chan struct{})
	go pc.loop()
}

// Stop はバックグランド goroutine を停止し、終了を待つ
func (pc *PageCleaner) Stop() {
	if pc.done == nil {
		return
	}
	close(pc.done)
	<-pc.stopped
	pc.ticker.Stop()
	pc.done = nil
}

// loop はバックグラウンドで定期的に clean を呼び出す
func (pc *PageCleaner) loop() {
	defer close(pc.stopped)
	for {
		select {
		case <-pc.done:
			return
		case <-pc.ticker.C:
			pc.clean()
		}
	}
}

// clean はフラッシュの必要がある場合にフラッシュリストの古いページからフラッシュし、チェックポイントを実行する
func (pc *PageCleaner) clean() error {
	shouldFlush, err := pc.shouldFlush()
	if err != nil {
		return err
	}
	if !shouldFlush {
		return nil
	}

	flushCount := max(pc.bufferPool.NumOfFlushListPage()/4, 1)
	if err := pc.bufferPool.FlushOldestPages(flushCount); err != nil {
		return err
	}
	return pc.checkpoint.Execute()
}

// shouldFlush は閾値 (以下のいずれか) を超えているかを判定する
//   - Redo ログサイズが redoLogMaxSize を超えている
//   - ダーティーページ率が maxDirtyPagePct を超えている
func (pc *PageCleaner) shouldFlush() (bool, error) {
	numOfFlushListPage := pc.bufferPool.NumOfFlushListPage()
	if numOfFlushListPage == 0 {
		return false, nil
	}

	// Redo ログサイズ
	size, err := pc.redoLog.Size()
	if err != nil {
		return false, err
	}
	if size > int64(pc.redoLogMaxSize) {
		return true, nil
	}

	// ダーティーページ率
	dirtyPct := numOfFlushListPage * 100 / pc.bufferPool.MaxNumOfPage
	return dirtyPct > pc.maxDirtyPagePct, nil
}

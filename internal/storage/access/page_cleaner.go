package access

import (
	"log"
	"sync"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

// PageCleaner はバックグラウンドでのダーティーページのフラッシュを管理する
type PageCleaner struct {
	bufferPool      *buffer.Pool
	redoLog         *redo.Buffer
	checkpoint      *Checkpoint
	redoLogMaxSize  int           // Redo ログの最大サイズ
	maxDirtyPagePct int           // ダーティーページ率の上限
	interval        time.Duration // クリーニング間隔
	ticker          *time.Ticker
	done            chan struct{}
	stopped         chan struct{} // goroutine 終了通知用
	stopOnce        sync.Once
	isRunning       bool
}

func NewPageCleaner(bp *buffer.Pool, redo *redo.Buffer, redoMaxSize int, maxDirtyPct int) *PageCleaner {
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
	if pc.isRunning {
		return
	}
	pc.ticker = time.NewTicker(pc.interval)
	pc.done = make(chan struct{})
	pc.stopped = make(chan struct{})
	pc.stopOnce = sync.Once{}
	pc.isRunning = true
	go pc.loop()
}

// Stop はバックグラウンド goroutine を停止し、終了を待つ
func (pc *PageCleaner) Stop() {
	pc.stopOnce.Do(func() {
		if !pc.isRunning {
			return
		}
		close(pc.done)
		<-pc.stopped
		pc.ticker.Stop()
		pc.isRunning = false
	})
}

// loop はバックグラウンドで定期的に clean を呼び出す
func (pc *PageCleaner) loop() {
	defer close(pc.stopped)
	for {
		select {
		case <-pc.done:
			return
		case <-pc.ticker.C:
			if err := pc.clean(); err != nil {
				log.Printf("page cleaner: %v", err)
			}
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

	// データページより先に Redo ログをディスクにフラッシュ
	if err := pc.redoLog.Flush(); err != nil {
		return err
	}

	// データページのフラッシュ
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
	dirtyPct := numOfFlushListPage * 100 / pc.bufferPool.MaxPages()
	return dirtyPct > pc.maxDirtyPagePct, nil
}

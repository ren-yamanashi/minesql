package buffer

import (
	stdlog "log"
	"minesql/internal/storage/log"
	"time"
)

// PageCleaner はバックグラウンドでダーティーページのフラッシュを管理する
//
// 一定間隔で閾値を確認し、超えている場合にフラッシュリストの古いページからフラッシュする
type PageCleaner struct {
	bp              *BufferPool
	redoLog         *log.RedoLog
	checkpoint      *Checkpoint
	redoLogMaxSize  int           // REDO ログの最大サイズ (バイト)
	maxDirtyPagePct int           // ダーティーページ率の上限 (%)
	interval        time.Duration // クリーニング間隔
	ticker          *time.Ticker
	done            chan struct{}
	stopped         chan struct{} // goroutine 終了通知用
}

// NewPageCleaner は PageCleaner を生成する
func NewPageCleaner(bp *BufferPool, redoLog *log.RedoLog, redoLogMaxSize int, maxDirtyPagePct int) *PageCleaner {
	return &PageCleaner{
		bp:              bp,
		redoLog:         redoLog,
		checkpoint:      NewCheckpoint(bp, redoLog),
		redoLogMaxSize:  redoLogMaxSize,
		maxDirtyPagePct: maxDirtyPagePct,
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

// Stop はバックグラウンド goroutine を停止し、終了を待つ
func (pc *PageCleaner) Stop() {
	// Start() が呼ばれていない場合は何もしない
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
func (pc *PageCleaner) clean() {
	if !pc.shouldFlush() {
		return
	}
	flushCount := max(pc.bp.FlushListSize()/4, 1)
	if err := pc.bp.FlushOldestPages(flushCount); err != nil {
		stdlog.Printf("page cleaner: flush failed: %v", err)
		return
	}
	if err := pc.checkpoint.Execute(); err != nil {
		stdlog.Printf("page cleaner: checkpoint failed: %v", err)
	}
}

// shouldFlush は閾値 (以下のいずれか) を超えているかを判定する
//   - REDO ログサイズが redoLogMaxSize を超えている
//   - ダーティーページ率が maxDirtyPagePct を超えている
func (pc *PageCleaner) shouldFlush() bool {
	flushListSize := pc.bp.FlushListSize()
	if flushListSize == 0 {
		return false
	}

	// REDO ログサイズの閾値チェック
	fileSize, err := pc.redoLog.FileSize()
	if err != nil {
		fileSize = 0
	}
	if fileSize+int64(pc.redoLog.BufferSize()) > int64(pc.redoLogMaxSize) {
		return true
	}

	// ダーティーページ率の閾値チェック
	dirtyPct := flushListSize * 100 / pc.bp.MaxBufferSize()
	return dirtyPct > pc.maxDirtyPagePct
}

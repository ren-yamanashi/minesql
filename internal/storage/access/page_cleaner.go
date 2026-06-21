package access

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
)

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
	flushRequest    chan struct{} // バッファプールが追い出し候補を確保できないときのフラッシュ通知
	stopOnce        sync.Once
	isRunning       atomic.Bool
}

func NewPageCleaner(
	bp *buffer.Pool,
	redo *redo.Buffer,
	trxMgr *TrxManager,
	redoMaxSize int,
	maxDirtyPct int,
) *PageCleaner {
	return &PageCleaner{
		bufferPool:      bp,
		redoLog:         redo,
		checkpoint:      NewCheckpoint(bp, redo, trxMgr),
		redoLogMaxSize:  redoMaxSize,
		maxDirtyPagePct: maxDirtyPct,
		interval:        1 * time.Second,
		flushRequest:    make(chan struct{}, 1),
	}
}

// RequestFlush はバッファプールが追い出し候補を確保できないときに即時フラッシュを発火させる
func (pc *PageCleaner) RequestFlush() {
	select {
	case pc.flushRequest <- struct{}{}:
	default:
	}
}

func (pc *PageCleaner) Start() {
	if !pc.isRunning.CompareAndSwap(false, true) {
		return
	}
	pc.ticker = time.NewTicker(pc.interval)
	pc.done = make(chan struct{})
	pc.stopped = make(chan struct{})
	pc.flushRequest = make(chan struct{}, 1)
	pc.stopOnce = sync.Once{}
	go pc.loop()
}

func (pc *PageCleaner) Stop() {
	pc.stopOnce.Do(func() {
		if !pc.isRunning.Load() {
			return
		}
		close(pc.done)
		<-pc.stopped
		pc.ticker.Stop()
		pc.isRunning.Store(false)
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
		case <-pc.flushRequest:
			// 追い出し詰まりを解消するため、閾値に関わらずダーティーページがあればフラッシュする
			if err := pc.flush(); err != nil {
				log.Printf("page cleaner: %v", err)
			}
		}
	}
}

// clean は閾値を超えている場合にフラッシュリストの古いページからフラッシュし、チェックポイントを実行する
func (pc *PageCleaner) clean() error {
	shouldFlush, err := pc.shouldFlush()
	if err != nil {
		return err
	}
	if !shouldFlush {
		return nil
	}
	return pc.flush()
}

// flush はダーティーページが存在する場合に、Redo ログ→データページの順でフラッシュし、チェックポイントを実行する
func (pc *PageCleaner) flush() error {
	if pc.bufferPool.FlushListPageCount() == 0 {
		return nil
	}

	// データページより先に Redo ログをディスクにフラッシュ
	if err := pc.redoLog.Flush(); err != nil {
		return err
	}

	// データページのフラッシュ
	flushCount := max(pc.bufferPool.FlushListPageCount()/4, 1)
	if err := pc.bufferPool.FlushOldestPages(flushCount); err != nil {
		return err
	}
	return pc.checkpoint.Execute()
}

// shouldFlush は閾値 (以下のいずれか) を超えているかを判定する
//   - Redo ログサイズが redoLogMaxSize を超えている
//   - ダーティーページ率が maxDirtyPagePct を超えている
func (pc *PageCleaner) shouldFlush() (bool, error) {
	numOfFlushListPage := pc.bufferPool.FlushListPageCount()
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

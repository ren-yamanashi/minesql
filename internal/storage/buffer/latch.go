package buffer

import "sync"

// LatchMode はラッチの取得モード
type LatchMode int

const (
	LatchShared LatchMode = iota + 1
	LatchSharedExclusive
	LatchExclusive
)

// RWLatch はページや B+Tree などの物理的なデータ構造を守る短期排他用のラッチ
//   - 待機者は FIFO 順に付与され、後続の互換要求が先行 Exclusive を追い越さない
type RWLatch struct {
	mu        sync.Mutex
	cond      *sync.Cond
	sharedCnt int
	sxHeld    bool
	xHeld     bool
	waitQueue []*latchRequest
}

type latchRequest struct {
	mode    LatchMode
	granted bool
}

func NewRWLatch() *RWLatch {
	l := &RWLatch{}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// LockShared は Shared モードでラッチを取得する
func (l *RWLatch) LockShared() { l.lock(LatchShared) }

// LockSharedExclusive は Shared-Exclusive モードでラッチを取得する
func (l *RWLatch) LockSharedExclusive() { l.lock(LatchSharedExclusive) }

// LockExclusive は Exclusive モードでラッチを取得する
func (l *RWLatch) LockExclusive() { l.lock(LatchExclusive) }

// TryLockExclusive は即時に Exclusive ラッチの取得を試み、取得できたら true を返す
//   - 待機キューに既に他者がいる場合や保持中の他者がいる場合は false を返し並ばない
func (l *RWLatch) TryLockExclusive() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.waitQueue) > 0 || !l.isCompatible(LatchExclusive) {
		return false
	}
	l.acquire(LatchExclusive)
	return true
}

// Unlock は指定モードのラッチを解放する
func (l *RWLatch) Unlock(mode LatchMode) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.release(mode)
	l.processWaitQueue()
	l.cond.Broadcast()
}

func (l *RWLatch) lock(mode LatchMode) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 待機者がいない場合のみ即時付与を試みる (FIFO 公平性のため、待機者がいるときは並ばせる)
	if len(l.waitQueue) == 0 && l.isCompatible(mode) {
		l.acquire(mode)
		return
	}

	req := &latchRequest{mode: mode}
	l.waitQueue = append(l.waitQueue, req)
	for !req.granted {
		l.cond.Wait()
	}
}

// isCompatible は現在の保持状態に対して指定モードを付与可能か判定する
func (l *RWLatch) isCompatible(mode LatchMode) bool {
	if l.xHeld {
		return false
	}
	switch mode {
	case LatchShared:
		return true
	case LatchSharedExclusive:
		return !l.sxHeld
	case LatchExclusive:
		return l.sharedCnt == 0 && !l.sxHeld
	}
	return false
}

func (l *RWLatch) acquire(mode LatchMode) {
	switch mode {
	case LatchShared:
		l.sharedCnt++
	case LatchSharedExclusive:
		l.sxHeld = true
	case LatchExclusive:
		l.xHeld = true
	}
}

func (l *RWLatch) release(mode LatchMode) {
	switch mode {
	case LatchShared:
		l.sharedCnt--
	case LatchSharedExclusive:
		l.sxHeld = false
	case LatchExclusive:
		l.xHeld = false
	}
}

// processWaitQueue は待機キューの先頭から順に付与可能なリクエストへラッチを付与する
//   - 先頭が付与不能になった時点でループを抜ける (公平性)
//   - Exclusive を付与したら以降は互換性で弾かれるためループを抜ける
func (l *RWLatch) processWaitQueue() {
	for len(l.waitQueue) > 0 {
		req := l.waitQueue[0]
		if !l.isCompatible(req.mode) {
			return
		}
		l.acquire(req.mode)
		req.granted = true
		l.waitQueue = l.waitQueue[1:]
		if req.mode == LatchExclusive {
			return
		}
	}
}

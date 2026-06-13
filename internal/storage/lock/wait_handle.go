package lock

import "time"

// WaitHandle は TryLock が即時付与できなかったロック要求の待機ハンドル
//   - TryLock が待機キューへ登録したリクエストに対応する
//   - 呼び出し側はページラッチなどを解放してから Wait を呼ぶ
type WaitHandle struct {
	manager *Manager
	state   *state
	key     rowLockKey
	trxId   TrxId
	mode    Mode
}

// Wait はロックが付与されるか、タイムアウトするまで待機する
//   - 呼び出し前に B+Tree のページラッチ・レベルラッチを解放しておくこと
//   - 付与された場合は nil、タイムアウトした場合は ErrTimeout を返す
func (h *WaitHandle) Wait() error {
	m := h.manager
	m.mu.Lock()
	defer m.mu.Unlock()

	timedOut := false
	timer := time.AfterFunc(m.timeout, func() {
		m.mu.Lock()
		timedOut = true
		m.cond.Broadcast()
		m.mu.Unlock()
	})
	defer timer.Stop()

	for {
		held, exists := h.state.holders[h.trxId]
		isGranted := exists && (held == Exclusive || h.mode == Shared)
		if isGranted {
			m.addHeldLock(h.trxId, h.key)
			return nil
		}
		if timedOut {
			m.removeFromWaitQueue(h.state, h.trxId)
			return ErrTimeout
		}
		m.cond.Wait()
	}
}

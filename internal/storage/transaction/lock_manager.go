package transaction

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrLockTimeout = errors.New("lock wait timeout")
)

// rowId は各行に割り当てられる一意の ID。実態は `ページ ID + スロット番号`
type rowId struct {
	pageId  int // 該当行が格納されているページの ID
	slotNum int // 該当行がページ内のどのスロットに格納されているか
}

type LockManager struct {
	lockTable map[rowId]*LockState // rowId ごとのロック状態を管理するマップ
	mu        sync.Mutex           // lockTable への同時アクセスを防ぐための mutex
	cond      *sync.Cond           // ロックの状態変化を待ち受けるための条件変数
	timeout   time.Duration        // ロック取得のタイムアウト値
}

func NewLockManager(timeoutMs int) *LockManager {
	lm := &LockManager{
		lockTable: make(map[rowId]*LockState),
		timeout:   time.Duration(timeoutMs) * time.Millisecond,
	}
	lm.cond = sync.NewCond(&lm.mu)
	return lm
}

// Lock は指定されたトランザクション ID と行 ID に対して、指定されたロックモードのロック取得を試みる
//
// 競合がなければ即座にロックを付与する。競合がある場合は待機キューに追加し、
// ロックが付与されるかタイムアウトするまで待機する
func (lm *LockManager) Lock(trxId TrxId, rId rowId, mode lockMode) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	state, exists := lm.lockTable[rId]

	// ロック状態が存在しない場合は新規作成
	if !exists {
		state = &LockState{
			handlers:  make(map[TrxId]lockMode),
			waitQueue: nil,
		}
		lm.lockTable[rId] = state
	}

	// 既に適切なロックを保持している場合は何もしない
	if heldMode, ok := state.handlers[trxId]; ok {
		if heldMode == exclusiveMode || mode == sharedMode {
			return nil
		}
	}

	// 競合がない場合は即座に付与
	if state.CanGrant(trxId, mode) {
		state.handlers[trxId] = mode
		return nil
	}

	// 競合がある場合は待機キューに追加
	state.waitQueue = append(state.waitQueue, &LockRequest{trxId: trxId, mode: mode})

	// タイムアウト用のタイマーを起動
	timedOut := false
	timer := time.AfterFunc(lm.timeout, func() {
		lm.mu.Lock()
		timedOut = true
		lm.cond.Broadcast()
		lm.mu.Unlock()
	})
	defer timer.Stop()

	// ロックが付与されるかタイムアウトするまで待機
	for {
		// grantWaitingLocks によってロックが付与されたか確認
		if heldMode, ok := state.handlers[trxId]; ok && (heldMode == exclusiveMode || mode == sharedMode) {
			return nil
		}
		if timedOut {
			lm.removeFromWaitQueue(state, trxId)
			return ErrLockTimeout
		}
		lm.cond.Wait()
	}
}

// UnlockAll は指定されたトランザクション ID が保持しているすべてのロックを解放する
//
// 解放後、待機キュー内のリクエストに対してロックの付与を試みる
func (lm *LockManager) UnlockAll(trxId TrxId, heldLocks []rowId) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for _, rId := range heldLocks {
		state, exists := lm.lockTable[rId]
		if !exists {
			continue
		}
		delete(state.handlers, trxId)
		lm.grantWaitingLocks(state)

		// 保持者も待機者もいなければエントリを削除
		if len(state.handlers) == 0 && len(state.waitQueue) == 0 {
			delete(lm.lockTable, rId)
		}
	}

	lm.cond.Broadcast()
}

// grantWaitingLocks は待機キューの先頭から順にロック付与を試みる
//
// 排他ロックの待機者に到達した時点で付与を停止する (FIFO 順序を保証)
func (lm *LockManager) grantWaitingLocks(state *LockState) {
	i := 0
	for i < len(state.waitQueue) {
		req := state.waitQueue[i]

		// ロックを付与できるか判定
		canGrant := false

		// ロック保持者がいない場合は付与可能
		if len(state.handlers) == 0 {
			canGrant = true
		} else {
			switch req.mode {
			case sharedMode:
				// 共有ロックは、現在の保持者と競合しなければ付与可能
				canGrant = state.IsCompatible(sharedMode)
			case exclusiveMode:
				// 排他ロックは、保持者が空か、自身が唯一の保持者 (昇格) の場合のみ付与可能
				_, holdsLock := state.handlers[req.trxId]
				canGrant = len(state.handlers) == 1 && holdsLock
			}
		}

		// ロックを付与できる場合は、保持者に追加して待機キューから削除
		if canGrant {
			state.handlers[req.trxId] = req.mode
			state.waitQueue = append(state.waitQueue[:i], state.waitQueue[i+1:]...)
		} else {
			// 排他ロックの待機者がいたら、それ以降の共有ロックも付与しない (FIFO)
			if req.mode == exclusiveMode {
				break
			}
			i++
		}
	}
}

// removeFromWaitQueue は待機キューから指定したトランザクションのリクエストを削除する
func (lm *LockManager) removeFromWaitQueue(state *LockState, trxId TrxId) {
	for i, req := range state.waitQueue {
		if req.trxId == trxId {
			state.waitQueue = append(state.waitQueue[:i], state.waitQueue[i+1:]...)
			return
		}
	}
}

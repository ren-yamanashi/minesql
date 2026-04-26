package lock

import (
	"errors"
	"sync"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

var ErrTimeout = errors.New("lock wait timeout")

// Manager は行レベルロックを管理する
type Manager struct {
	lockTable map[page.SlotPosition]*lockState // 行ごとのロック状態を管理するマップ
	mutex     sync.Mutex                       // lockTable への同時アクセスを防ぐための mutex
	heldLocks map[TrxId][]page.SlotPosition    // トランザクションごとのロック保持行リスト
	cond      *sync.Cond                       // ロックの状態変化を待ち受けるための条件変数
	timeout   time.Duration                    // ロック取得のタイムアウト値
}

func NewManager(timeoutMs int) *Manager {
	m := &Manager{
		lockTable: make(map[page.SlotPosition]*lockState),
		heldLocks: make(map[TrxId][]page.SlotPosition),
		timeout:   time.Duration(timeoutMs) * time.Millisecond,
	}
	m.cond = sync.NewCond(&m.mutex)
	return m
}

// Lock は指定した行に対してロックを取得する
//
// 競合がなければ即座にロックを付与する。競合がある場合は待機キューに追加し、
// ロックが付与されるかタイムアウトするまで待機する
func (m *Manager) Lock(trxId TrxId, pos page.SlotPosition, mode LockMode) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	state, exists := m.lockTable[pos]

	// ロック状態が存在しない場合は新規作成
	if !exists {
		state = newLockState()
		m.lockTable[pos] = state
	}

	// 既に適切なロックを保持している場合は何もしない
	if held, ok := state.holders[trxId]; ok {
		if held == Exclusive || mode == Shared {
			return nil
		}
	}

	// 競合がない場合は即座に付与
	if state.canGrant(trxId, mode) {
		state.holders[trxId] = mode
		m.recordHeldLock(trxId, pos)
		return nil
	}

	// 競合がある場合は待機キューに追加
	state.waitQueue = append(state.waitQueue, &lockRequest{trxId: trxId, mode: mode})

	// タイムアウト用のタイマーを起動
	timedOut := false
	timer := time.AfterFunc(m.timeout, func() {
		m.mutex.Lock()
		timedOut = true
		m.cond.Broadcast()
		m.mutex.Unlock()
	})
	defer timer.Stop()

	// ロックが付与されるかタイムアウトするまで待機
	for {
		// grantWaitingLocks によってロックが付与されたか確認
		if held, ok := state.holders[trxId]; ok && (held == Exclusive || mode == Shared) {
			m.recordHeldLock(trxId, pos)
			return nil
		}
		if timedOut {
			m.removeFromWaitQueue(state, trxId)
			return ErrTimeout
		}
		m.cond.Wait()
	}
}

// ReleaseAll は指定したトランザクションが保持している全ロックを解放する
//
// 解放後、待機キュー内のリクエストに対してロックの付与を試みる
func (m *Manager) ReleaseAll(trxId TrxId) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, pos := range m.heldLocks[trxId] {
		state, exists := m.lockTable[pos]
		if !exists {
			continue
		}
		delete(state.holders, trxId)
		m.grantWaitingLocks(state)

		// 保持者も待機者もいなければエントリを削除
		if len(state.holders) == 0 && len(state.waitQueue) == 0 {
			delete(m.lockTable, pos)
		}
	}
	delete(m.heldLocks, trxId)

	m.cond.Broadcast()
}

func (m *Manager) recordHeldLock(trxId TrxId, pos page.SlotPosition) {
	for _, existing := range m.heldLocks[trxId] {
		if existing == pos {
			return
		}
	}
	m.heldLocks[trxId] = append(m.heldLocks[trxId], pos)
}

// grantWaitingLocks は待機キューの先頭から順にロック付与を試みる
//
// 排他ロックの待機者に到達した時点で付与を停止する (FIFO 順序を保証)
func (m *Manager) grantWaitingLocks(state *lockState) {
	i := 0
	for i < len(state.waitQueue) {
		req := state.waitQueue[i]

		// ロックを付与できるか判定
		canGrant := false

		// ロック保持者がいない場合は付与可能
		if len(state.holders) == 0 {
			canGrant = true
		} else {
			switch req.mode {
			case Shared:
				// 共有ロックは、現在の保持者と競合しなければ付与可能
				canGrant = state.isCompatible(Shared)
			case Exclusive:
				// 排他ロックは、保持者が空か、自身が唯一の保持者 (昇格) の場合のみ付与可能
				_, holds := state.holders[req.trxId]
				canGrant = len(state.holders) == 1 && holds
			}
		}

		// ロックを付与できる場合は、保持者に追加して待機キューから削除
		if canGrant {
			state.holders[req.trxId] = req.mode
			state.waitQueue = append(state.waitQueue[:i], state.waitQueue[i+1:]...)
		} else {
			// 排他ロックの待機者がいたら、それ以降の共有ロックも付与しない (FIFO)
			if req.mode == Exclusive {
				break
			}
			i++
		}
	}
}

// removeFromWaitQueue は待機キューから指定したトランザクションのリクエストを削除する
func (m *Manager) removeFromWaitQueue(state *lockState, trxId TrxId) {
	for i, req := range state.waitQueue {
		if req.trxId == trxId {
			state.waitQueue = append(state.waitQueue[:i], state.waitQueue[i+1:]...)
			return
		}
	}
}

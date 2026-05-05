package lock

import (
	"errors"
	"sync"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
)

var ErrTimeout = errors.New("lock wait timeout")

// Manager は行レベルロックを管理する
type Manager struct {
	lockTable map[node.RecordPosition]*state  // レコード位置 → ロック状態のマップ
	mutex     sync.Mutex                      // lockTable への同時アクセスを防ぐための mutex
	heldLocks map[TrxId][]node.RecordPosition // トランザクションごとのロック保持レコードリスト
	cond      *sync.Cond                      // ロックの状態変化を待ち受けるための条件変数
	timeout   time.Duration                   // ロック取得のタイムアウト値
}

func NewManager() *Manager {
	lm := &Manager{
		lockTable: make(map[node.RecordPosition]*state),
		heldLocks: make(map[TrxId][]node.RecordPosition),
		timeout:   time.Duration(config.LockWaitTimeoutMs) * time.Millisecond,
	}
	lm.cond = sync.NewCond(&lm.mutex)
	return lm
}

// Lock は指定した行に対してロックを取得する
//   - 競合がなければ即座にロックを付与する
//   - 競合がある場合は待機キューに追加し、ロックが付与されるかタイムアウトするまで待機する
func (m *Manager) Lock(trxId TrxId, pos node.RecordPosition, mode Mode) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	state, exists := m.lockTable[pos]
	if !exists {
		state = newState()
		m.lockTable[pos] = state
	}

	// 既に適切なロックを保持している場合
	if held, ok := state.holders[trxId]; ok && (held == Exclusive || mode == Shared) {
		return nil
	}

	// 競合がない場合
	if state.canGrant(trxId, mode) {
		state.holders[trxId] = mode
		m.appendRecordHeldLock(trxId, pos)
		return nil
	}

	// 競合がある場合
	state.waitQueue = append(state.waitQueue, &request{trxId: trxId, mode: mode})

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
		if held, ok := state.holders[trxId]; ok && (held == Exclusive || mode == Shared) {
			m.appendRecordHeldLock(trxId, pos)
			return nil
		}
		if timedOut {
			m.removeFromWaitQueue(state, trxId)
			return ErrTimeout
		}
		m.cond.Wait()
	}
}

// Release は指定したトランザクションが保持しているすべてのロックを解放する
func (m *Manager) Release(trxId TrxId) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, pos := range m.heldLocks[trxId] {
		state, exists := m.lockTable[pos]
		if !exists {
			continue
		}
		delete(state.holders, trxId)
		m.grantWaitingLocks(state)

		// 保持者が誰もいなければ削除
		if len(state.holders) == 0 && len(state.waitQueue) == 0 {
			delete(m.lockTable, pos)
		}
	}

	delete(m.heldLocks, trxId)
	m.cond.Broadcast()
}

// grantWaitingLocks は待機キューの先頭から順にロック付与を試みる
func (m *Manager) grantWaitingLocks(state *state) {
	i := 0
	for i < len(state.waitQueue) {
		request := state.waitQueue[i]
		canGrant := false

		// ロック保持者がいない場合
		if len(state.holders) == 0 {
			canGrant = true
		} else {
			switch request.mode {
			case Shared:
				canGrant = state.isCompatible(Shared)
			case Exclusive:
				// 保持者が空か、自身が唯一の保持者 (Shared->Exclusive の昇格) の場合のみ付与可能
				_, holds := state.holders[request.trxId]
				canGrant = len(state.holders) == 1 && holds
			}
		}

		if canGrant {
			// grantWaitingLocks によってロックが付与されたか確認
			state.holders[request.trxId] = request.mode
			state.waitQueue = append(state.waitQueue[:i], state.waitQueue[i+1:]...)
			continue
		}
		// 排他ロックの待機者にロックを付与できない場合、後続のロック(Shared 含む)に対しても付与しない
		if request.mode == Exclusive {
			break
		}
		i++
	}
}

// appendRecordHeldLock は、指定したトランザクションのロック保持リストにレコードを登録する
func (m *Manager) appendRecordHeldLock(trxId TrxId, pos node.RecordPosition) {
	for _, existing := range m.heldLocks[trxId] {
		if existing == pos {
			return
		}
	}
	m.heldLocks[trxId] = append(m.heldLocks[trxId], pos)
}

// removeFromWaitQueue は待機キューから指定したトランザクションのリクエストを削除する
func (m *Manager) removeFromWaitQueue(state *state, trxId TrxId) {
	for i, request := range state.waitQueue {
		if request.trxId == trxId {
			state.waitQueue = append(state.waitQueue[:i], state.waitQueue[i+1:]...)
			return
		}
	}
}

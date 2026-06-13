package lock

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/config"
)

var ErrTimeout = errors.New("lock wait timeout")

// Manager は行レベルロックを管理する
type Manager struct {
	lockTable map[rowLockKey]*state  // 行ロック識別子 → ロック状態のマップ
	mu        sync.Mutex             // lockTable への同時アクセスを防ぐための mutex
	heldLocks map[TrxId][]rowLockKey // トランザクションごとのロック保持リスト
	cond      *sync.Cond             // ロックの状態変化を待ち受けるための条件変数
	timeout   time.Duration          // ロック取得のタイムアウト値
}

func NewManager() *Manager {
	lm := &Manager{
		lockTable: make(map[rowLockKey]*state),
		heldLocks: make(map[TrxId][]rowLockKey),
		timeout:   config.LockWaitTimeout,
	}
	lm.cond = sync.NewCond(&lm.mu)
	return lm
}

// TryLock は指定した行に対してロックの即時付与を試みる
//   - 即時付与できた場合は granted=true, handle=nil を返す
//   - 競合する場合は待機キューに登録し、granted=false と待機ハンドルを返す
//   - 即時付与の判定と待機キューへの登録を 1 つのロック区間で原子的に行うため、判定と登録の間に他トランザクションが割り込んで待機キューの順序が乱れることはない
//   - 付与できなかった場合、呼び出し側はページラッチなどを解放してから handle.Wait で待機する
func (m *Manager) TryLock(trxId TrxId, rowKey RowKey, mode Mode) (granted bool, handle *WaitHandle) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := newRowLockKey(rowKey)
	state, exists := m.lockTable[key]
	if !exists {
		state = newState()
		m.lockTable[key] = state
	}

	// 競合がない場合 (既に適切なロックを保持している場合も含む)
	if state.canGrant(trxId, mode) {
		// 既存保持モードが要求モードより強い (Exclusive を保持中に Shared 要求) 場合は上書きしない
		held, exists := state.holders[trxId]
		if !exists || held != Exclusive || mode != Shared {
			state.holders[trxId] = mode
		}
		m.addHeldLock(trxId, key)
		return true, nil
	}

	// 競合がある場合: 待機キューに登録して待機ハンドルを返す
	state.waitQueue = append(state.waitQueue, request{trxId: trxId, mode: mode})
	return false, &WaitHandle{
		manager: m,
		state:   state,
		key:     key,
		trxId:   trxId,
		mode:    mode,
	}
}

// Lock は指定した行に対してロックを取得する
//   - 競合がなければ即座にロックを付与する
//   - 競合がある場合は待機キューに追加し、ロックが付与されるかタイムアウトするまで待機する
func (m *Manager) Lock(trxId TrxId, rowKey RowKey, mode Mode) error {
	granted, handle := m.TryLock(trxId, rowKey, mode)
	if granted {
		return nil
	}
	return handle.Wait()
}

// Release は指定したトランザクションが保持しているすべてのロックを解放する
func (m *Manager) Release(trxId TrxId) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range m.heldLocks[trxId] {
		state, exists := m.lockTable[key]
		if !exists {
			continue
		}
		delete(state.holders, trxId)
		m.grantWaitingLocks(state)

		// 保持者が誰もいなければ削除
		if len(state.holders) == 0 && len(state.waitQueue) == 0 {
			delete(m.lockTable, key)
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
			state.waitQueue = slices.Delete(state.waitQueue, i, i+1)
			continue
		}
		// 排他ロックの待機者にロックを付与できない場合、後続のロック(Shared 含む)に対しても付与しない
		if request.mode == Exclusive {
			break
		}
		i++
	}
}

// addHeldLock は指定したトランザクションのロック保持リストに行ロック識別子を追加する
func (m *Manager) addHeldLock(trxId TrxId, key rowLockKey) {
	if slices.Contains(m.heldLocks[trxId], key) {
		return
	}
	m.heldLocks[trxId] = append(m.heldLocks[trxId], key)
}

// removeFromWaitQueue は待機キューから指定したトランザクションのリクエストを削除する
func (m *Manager) removeFromWaitQueue(state *state, trxId TrxId) {
	state.waitQueue = slices.DeleteFunc(state.waitQueue, func(r request) bool {
		return r.trxId == trxId
	})
}

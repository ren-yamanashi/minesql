package access

import (
	"slices"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type trxState int

const (
	trxStateActive trxState = iota + 1
	trxStateInactive
)

type TrxManager struct {
	mu           sync.RWMutex
	undoLog      *undo.Manager
	redoLog      *redo.Buffer
	lock         *lock.Manager
	bufferPool   *buffer.Pool
	catalog      *catalog.Catalog
	transactions map[lock.TrxId]trxState
	readViews    map[lock.TrxId]*readView // トランザクションごとの ReadView キャッシュ
	nextTrxId    lock.TrxId               // 次に払い出すトランザクション ID
}

func NewTrxManager(
	ct *catalog.Catalog,
	undo *undo.Manager,
	redo *redo.Buffer,
	lockMgr *lock.Manager,
	bp *buffer.Pool,
) *TrxManager {
	return &TrxManager{
		undoLog:      undo,
		redoLog:      redo,
		lock:         lockMgr,
		bufferPool:   bp,
		catalog:      ct,
		transactions: make(map[lock.TrxId]trxState),
		readViews:    make(map[lock.TrxId]*readView),
	}
}

// Begin は新しいトランザクションを開始し、トランザクション ID を返す
func (t *TrxManager) Begin() lock.TrxId {
	t.mu.Lock()
	defer t.mu.Unlock()

	trxId := t.allocateTrxId()
	t.transactions[trxId] = trxStateActive
	return trxId
}

// Commit はトランザクションをコミットし、ロックを開放して Undo ログを破棄する
func (t *TrxManager) Commit(trxId lock.TrxId) error {
	// Redo ログに Commit レコードを記録してフラッシュ
	t.redoLog.AppendCommit(trxId)
	if err := t.redoLog.Flush(); err != nil {
		return err
	}

	// コミット後はロックを開放して INSERT の Undo ログを破棄
	// UPDATE/DELETE の Undo レコードは他のトランザクションの ReadView から Undo チェーン辿りに必要
	t.lock.Release(trxId)
	t.undoLog.DiscardRecordType(trxId, undo.RecordTypeInsert)

	t.mu.Lock()
	delete(t.readViews, trxId)
	t.transactions[trxId] = trxStateInactive
	t.mu.Unlock()

	return nil
}

// Rollback は Undo ログを逆順に適用してトランザクションをロールバックし、ロックを開放する
func (t *TrxManager) Rollback(trxId lock.TrxId) error {
	defer func() {
		t.lock.Release(trxId)
		t.undoLog.Discard(trxId)

		t.mu.Lock()
		delete(t.readViews, trxId)
		t.transactions[trxId] = trxStateInactive
		t.mu.Unlock()
	}()

	// Redo ログに Rollback レコードを記録 (フラッシュなし)
	t.redoLog.AppendRollback(trxId)

	records := t.undoLog.Records(trxId)
	for _, r := range slices.Backward(records) {
		if err := t.rollbackRecord(r); err != nil {
			return err
		}
	}
	return nil
}

// CreateReadView は指定したトランザクション用の ReadView を作成する
func (t *TrxManager) CreateReadView(trxId lock.TrxId) *readView {
	t.mu.Lock()
	defer t.mu.Unlock()

	// REPEATABLE READ のみのため、同一トランザクション内では最初に作成した ReadView をキャッシュして使い回す
	if rv, ok := t.readViews[trxId]; ok {
		return rv
	}
	var activeTrxIds []lock.TrxId
	for id, state := range t.transactions {
		if state == trxStateActive && id != trxId {
			activeTrxIds = append(activeTrxIds, id)
		}
	}
	rv := newReadView(trxId, activeTrxIds, t.nextTrxId)
	t.readViews[trxId] = rv
	return rv
}

// OldestVisibleTrxId は全アクティブ ReadView の upLimitId の最小値を返す
//   - この値未満の trxId は、どの ReadView からも参照されない
//   - アクティブな ReadView がない場合は nextTrxId を返す
func (t *TrxManager) OldestVisibleTrxId() lock.TrxId {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// アクティブな ReadView がない場合は nextTrxId を返す (全コミット済みトランザクションがパージ可能)
	if len(t.readViews) == 0 {
		return t.nextTrxId
	}
	limit := t.nextTrxId
	for _, rv := range t.readViews {
		limit = min(limit, rv.upLimitId)
	}
	return limit
}

// activeTrxIds はアクティブなトランザクションの ID 一覧を返す
func (t *TrxManager) activeTrxIds() []lock.TrxId {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var ids []lock.TrxId
	for id, state := range t.transactions {
		if state == trxStateActive {
			ids = append(ids, id)
		}
	}
	return ids
}

// InactiveTrxIds は完了済み (コミットまたはロールバック済み) のトランザクション ID 一覧を返す
func (t *TrxManager) InactiveTrxIds() []lock.TrxId {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var ids []lock.TrxId
	for trxId, state := range t.transactions {
		if state == trxStateInactive {
			ids = append(ids, trxId)
		}
	}
	return ids
}

// allocateTrxId はトランザクション ID を払い出す
func (t *TrxManager) allocateTrxId() lock.TrxId {
	id := t.nextTrxId
	t.nextTrxId++
	return id
}

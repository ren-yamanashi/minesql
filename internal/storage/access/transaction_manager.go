package access

import (
	"slices"
	"sync"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type TrxManager struct {
	mu           sync.RWMutex
	undoLog      *undo.Manager
	redoLog      *redo.Buffer
	lock         *lock.Manager
	bufferPool   *buffer.Pool
	catalog      *dictionary.Catalog
	transactions map[lock.TrxId]*Transaction
	nextTrxId    lock.TrxId // 次に払い出すトランザクション ID
}

func NewTrxManager(
	ct *dictionary.Catalog,
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
		transactions: make(map[lock.TrxId]*Transaction),
		nextTrxId:    1, // 0 は「未割り当て / 太古のコミット済み」の予約値のため使用しない
	}
}

// Begin は新しいトランザクションを開始し、Transaction オブジェクトを返す
func (t *TrxManager) Begin() *Transaction {
	t.mu.Lock()
	defer t.mu.Unlock()

	trxId := t.allocateTrxId()
	trx := &Transaction{
		trxId: trxId,
		state: trxStateActive,
		tm:    t,
	}
	t.transactions[trxId] = trx
	return trx
}

// Commit はトランザクションをコミットし、ロックを開放して Undo ログを破棄する
func (t *TrxManager) Commit(trx *Transaction) error {
	// Redo ログに Commit レコードを記録してフラッシュ
	if _, err := t.redoLog.AppendCommit(trx.trxId); err != nil {
		return err
	}
	if err := t.redoLog.Flush(); err != nil {
		return err
	}

	// コミット後はロックを開放して INSERT の Undo ログを破棄
	// UPDATE/DELETE の Undo レコードは他のトランザクションの ReadView から Undo チェーン辿りに必要
	t.lock.Release(trx.trxId)
	t.undoLog.DiscardRecordType(trx.trxId, undo.RecordTypeInsert)

	t.mu.Lock()
	trx.state = trxStateInactive
	trx.readView = nil
	t.mu.Unlock()

	return nil
}

// Rollback は Undo ログを逆順に適用してトランザクションをロールバックし、ロックを開放する
func (t *TrxManager) Rollback(trx *Transaction) error {
	defer func() {
		t.lock.Release(trx.trxId)
		t.undoLog.Discard(trx.trxId)

		t.mu.Lock()
		trx.state = trxStateInactive
		trx.readView = nil
		t.mu.Unlock()
	}()

	// Redo ログに Rollback レコードを記録 (フラッシュなし)
	if _, err := t.redoLog.AppendRollback(trx.trxId); err != nil {
		return err
	}

	records := t.undoLog.Records(trx.trxId)
	for _, r := range slices.Backward(records) {
		mtr := buffer.NewWriteMtr(t.bufferPool, trx.trxId, t.redoLog)
		if err := t.rollbackRecord(mtr, r); err != nil {
			mtr.UnpinAll()
			return err
		}
		if err := mtr.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// EnsureReadView は trx の ReadView が未作成なら作って返す。作成済みなら既存のものを返す
//   - 同一トランザクション内の 2 回目以降の参照は同じ ReadView が使い回される (REPEATABLE READ)
func (t *TrxManager) EnsureReadView(trx *Transaction) *readView {
	t.mu.Lock()
	defer t.mu.Unlock()

	if trx.readView != nil {
		return trx.readView
	}
	var activeTrxIds []lock.TrxId
	for id, other := range t.transactions {
		if id != trx.trxId && other.state == trxStateActive {
			activeTrxIds = append(activeTrxIds, id)
		}
	}
	trx.readView = newReadView(trx.trxId, activeTrxIds, t.nextTrxId)
	return trx.readView
}

// OldestVisibleTrxId は全アクティブ ReadView の upLimitId の最小値を返す
//   - この値未満の trxId は、どの ReadView からも参照されない
//   - アクティブな ReadView がない場合は nextTrxId を返す
func (t *TrxManager) OldestVisibleTrxId() lock.TrxId {
	t.mu.RLock()
	defer t.mu.RUnlock()

	limit := t.nextTrxId
	found := false
	for _, trx := range t.transactions {
		if trx.readView == nil {
			continue
		}
		limit = min(limit, trx.readView.upLimitId)
		found = true
	}
	if !found {
		return t.nextTrxId
	}
	return limit
}

// InactiveTrxIds は完了済み (コミットまたはロールバック済み) のトランザクション ID 一覧を返す
func (t *TrxManager) InactiveTrxIds() []lock.TrxId {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var ids []lock.TrxId
	for trxId, trx := range t.transactions {
		if trx.state == trxStateInactive {
			ids = append(ids, trxId)
		}
	}
	return ids
}

// activeTrxIds はアクティブなトランザクションの ID 一覧を返す
func (t *TrxManager) activeTrxIds() []lock.TrxId {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var ids []lock.TrxId
	for id, trx := range t.transactions {
		if trx.state == trxStateActive {
			ids = append(ids, id)
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

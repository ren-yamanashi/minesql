package access

import (
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type TrxState int

const (
	TrxStateActive TrxState = iota + 1
	TrxStateInactive
)

type TrxManager struct {
	undoLog      *undo.Manager
	lock         *lock.Manager
	bufferPool   *buffer.BufferPool
	catalog      *catalog.Catalog
	transactions map[lock.TrxId]TrxState
	readViews    map[lock.TrxId]*readView // トランザクションごとの ReadView キャッシュ
	nextTrxId    lock.TrxId               // 次に払い出すトランザクション ID
}

func NewTrxManager(ct *catalog.Catalog, undo *undo.Manager, lockMgr *lock.Manager, bp *buffer.BufferPool) *TrxManager {
	return &TrxManager{
		undoLog:      undo,
		lock:         lockMgr,
		bufferPool:   bp,
		catalog:      ct,
		transactions: make(map[lock.TrxId]TrxState),
		readViews:    make(map[lock.TrxId]*readView),
	}
}

// Begin は新しいトランザクションを開始し、トランザクション ID を返す
func (t *TrxManager) Begin() lock.TrxId {
	trxId := t.allocateTrxId()
	t.transactions[trxId] = TrxStateActive
	return trxId
}

// Commit はトランザクションをコミットし、ロックを開放して Undo ログを破棄する
func (t *TrxManager) Commit(trxId lock.TrxId) error {
	// TODO: Redo ログに Commit を記録する処理追加

	// コミット後はロックを開放して INSERT の Undo ログを破棄
	// UPDATE/DELETE の Undo レコードは他のトランザクションの ReadView から Undo チェーン辿りに必要
	t.lock.Release(trxId)
	t.undoLog.DiscardRecordType(trxId, undo.RecordTypeInsert)
	delete(t.readViews, trxId)
	t.transactions[trxId] = TrxStateInactive
	return nil
}

// Rollback は Undo ログを逆順に適用してトランザクションをロールバックし、ロックを開放する
func (t *TrxManager) Rollback(trxId lock.TrxId) error {
	defer func() {
		t.lock.Release(trxId)
		t.undoLog.Discard(trxId)
		delete(t.readViews, trxId)
		t.transactions[trxId] = TrxStateInactive
	}()

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
	// REPEATABLE READ のみのため、同一トランザクション内では最初に作成した ReadView をキャッシュして使い回す
	if rv, ok := t.readViews[trxId]; ok {
		return rv
	}
	var activeTrxIds []lock.TrxId
	for id, state := range t.transactions {
		if state == TrxStateActive && id != trxId {
			activeTrxIds = append(activeTrxIds, id)
		}
	}
	rv := newReadView(trxId, activeTrxIds, t.nextTrxId)
	t.readViews[trxId] = rv
	return rv
}

// PurgeLimit は全アクティブ ReadView の MUpLimitId の最小値を返す
//
// この値より小さい trxId のコミット済み undo ログおよび delete-marked レコードはパージ可能
func (t *TrxManager) PurgeLimit() lock.TrxId {
	// アクティブな ReadView がない場合は nextTrxId を返す (全コミット済みトランザクションがパージ可能)
	if len(t.readViews) == 0 {
		return t.nextTrxId
	}
	limit := t.nextTrxId
	for _, rv := range t.readViews {
		limit = min(limit, rv.MUpLimitId)
	}
	return limit
}

// ActiveTrxIds はアクティブなトランザクションの ID 一覧を返す
func (t *TrxManager) ActiveTrxIds() []lock.TrxId {
	var ids []lock.TrxId
	for id, state := range t.transactions {
		if state == TrxStateActive {
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

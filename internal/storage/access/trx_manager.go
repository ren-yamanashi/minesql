package access

import (
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/log"
)

// TrxId はトランザクション ID
type TrxId = uint64

type State string

const (
	StateActive   State = "ACTIVE"
	StateInactive State = "INACTIVE"
)

type TrxManager struct {
	undoLog      *UndoManager
	lockMgr      *lock.Manager
	redoLog      *log.RedoLog
	Transactions map[TrxId]State
	readViews    map[TrxId]*ReadView // トランザクションごとの ReadView キャッシュ
	nextTrxId    TrxId               // 次に払い出すトランザクション ID (単調増加)
}

func NewTrxManager(undoLog *UndoManager, lockMgr *lock.Manager, redoLog *log.RedoLog) *TrxManager {
	return &TrxManager{
		undoLog:      undoLog,
		lockMgr:      lockMgr,
		redoLog:      redoLog,
		Transactions: make(map[TrxId]State),
		readViews:    make(map[TrxId]*ReadView),
		nextTrxId:    1,
	}
}

// Begin は新しいトランザクションを開始し、トランザクション ID を返す
func (m *TrxManager) Begin() TrxId {
	trxId := m.allocateTrxId()
	m.Transactions[trxId] = StateActive
	return trxId
}

// Commit はトランザクションをコミットし、ロックを解放して Undo ログを破棄する
func (m *TrxManager) Commit(trxId TrxId) error {
	// REDO ログに COMMIT レコードを記録してフラッシュ
	if m.redoLog != nil {
		m.redoLog.AppendCommit(trxId)
		if err := m.redoLog.Flush(); err != nil {
			return err
		}
	}

	// コミット後はロックを解放して INSERT の Undo ログを破棄
	// UPDATE/DELETE の undo レコードは他トランザクションの ReadView から undo チェーン辿りに必要なため保持する
	m.lockMgr.ReleaseAll(trxId)
	m.undoLog.DiscardInsertRecords(trxId)
	delete(m.readViews, trxId)
	m.Transactions[trxId] = StateInactive
	return nil
}

// Rollback は Undo ログを逆順に適用してトランザクションをロールバックし、ロックを解放する
func (m *TrxManager) Rollback(bp *buffer.BufferPool, trxId TrxId) error {
	records := m.undoLog.GetRecords(trxId)
	for i := len(records) - 1; i >= 0; i-- {
		if err := records[i].Undo(bp, trxId, m.lockMgr); err != nil {
			return err
		}
	}
	// REDO ログに ROLLBACK レコードを記録 (フラッシュはしない)
	if m.redoLog != nil {
		m.redoLog.AppendRollback(trxId)
	}

	// ロールバック後はロックを解放して Undo ログを破棄
	m.lockMgr.ReleaseAll(trxId)
	m.undoLog.Discard(trxId)
	delete(m.readViews, trxId)
	m.Transactions[trxId] = StateInactive
	return nil
}

// CreateReadView は指定したトランザクション用の ReadView を返す
//
// REPEATABLE READ のため、同一トランザクション内では最初に作成した ReadView をキャッシュして使い回す
func (m *TrxManager) CreateReadView(trxId TrxId) *ReadView {
	if rv, ok := m.readViews[trxId]; ok {
		return rv
	}
	var activeTrxIds []TrxId
	for id, state := range m.Transactions {
		if state == StateActive && id != trxId {
			activeTrxIds = append(activeTrxIds, id)
		}
	}
	rv := NewReadView(trxId, activeTrxIds, m.nextTrxId)
	m.readViews[trxId] = rv
	return rv
}

func (m *TrxManager) allocateTrxId() TrxId {
	id := m.nextTrxId
	m.nextTrxId++
	return id
}

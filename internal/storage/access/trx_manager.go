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
}

func NewTrxManager(undoLog *UndoManager, lockMgr *lock.Manager, redoLog *log.RedoLog) *TrxManager {
	return &TrxManager{
		undoLog:      undoLog,
		lockMgr:      lockMgr,
		redoLog:      redoLog,
		Transactions: make(map[TrxId]State),
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

	// コミット後はロックを解放して Undo ログを破棄
	m.lockMgr.ReleaseAll(trxId)
	m.undoLog.Discard(trxId)
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
	m.Transactions[trxId] = StateInactive
	return nil
}

func (m *TrxManager) allocateTrxId() TrxId {
	var maxTrxId TrxId
	for trxId := range m.Transactions {
		if trxId > maxTrxId {
			maxTrxId = trxId
		}
	}
	return maxTrxId + 1
}

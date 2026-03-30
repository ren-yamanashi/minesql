package transaction

import (
	"minesql/internal/storage/buffer"
)

type State string

const (
	StateActive   State = "ACTIVE"
	StateInactive State = "INACTIVE"
)

type Manager struct {
	undoLog      *UndoLog
	Transactions map[TrxId]State
}

func NewManager(undoLog *UndoLog) *Manager {
	return &Manager{
		undoLog:      undoLog,
		Transactions: make(map[TrxId]State),
	}
}

// Begin は新しいトランザクションを開始し、トランザクション ID を返す
func (m *Manager) Begin() TrxId {
	trxId := m.allocateTrxId()
	m.Transactions[trxId] = StateActive
	return trxId
}

// Commit はトランザクションをコミットし、Undo ログを破棄する
func (m *Manager) Commit(trxId TrxId) {
	m.undoLog.Discard(trxId)
	m.Transactions[trxId] = StateInactive
}

// Rollback は Undo ログを逆順に適用してトランザクションをロールバックする
func (m *Manager) Rollback(bp *buffer.BufferPool, trxId TrxId) error {
	records := m.undoLog.GetRecords(trxId)
	for i := len(records) - 1; i >= 0; i-- {
		if err := records[i].Undo(bp); err != nil {
			return err
		}
	}
	m.undoLog.Discard(trxId)
	m.Transactions[trxId] = StateInactive
	return nil
}

func (m *Manager) allocateTrxId() TrxId {
	var maxTrxId TrxId
	for trxId := range m.Transactions {
		if trxId > maxTrxId {
			maxTrxId = trxId
		}
	}
	return maxTrxId + 1
}

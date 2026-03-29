package transaction

import "minesql/internal/undo"

type State string

const (
	StateActive   State = "ACTIVE"
	StateInactive State = "INACTIVE"
)

type Manager struct {
	undoLog      *undo.UndoLog
	Transactions map[undo.TrxId]State
}

func NewManager(undoLog *undo.UndoLog) *Manager {
	return &Manager{
		undoLog:      undoLog,
		Transactions: make(map[undo.TrxId]State),
	}
}

// Begin は新しいトランザクションを開始し、トランザクション ID を返す
func (m *Manager) Begin() undo.TrxId {
	trxId := m.allocateTrxId()
	m.Transactions[trxId] = StateActive
	return trxId
}

// Commit はトランザクションをコミットし、Undo ログを破棄する
func (m *Manager) Commit(trxId undo.TrxId) {
	m.undoLog.Discard(trxId)
	m.Transactions[trxId] = StateInactive
}

// Rollback は Undo ログを逆順に適用してトランザクションをロールバックする
func (m *Manager) Rollback(trxId undo.TrxId) error {
	records := m.undoLog.GetRecords(trxId)
	for i := len(records) - 1; i >= 0; i-- {
		if err := records[i].Undo(); err != nil {
			return err
		}
	}
	m.undoLog.Discard(trxId)
	m.Transactions[trxId] = StateInactive
	return nil
}

func (m *Manager) allocateTrxId() undo.TrxId {
	var maxTrxId undo.TrxId
	for trxId := range m.Transactions {
		if trxId > maxTrxId {
			maxTrxId = trxId
		}
	}
	return maxTrxId + 1
}

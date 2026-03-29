package executor

import "minesql/internal/undo"

type TrxId uint64

type TrxState string

const (
	TRX_ACTIVE   TrxState = "ACTIVE"
	TRX_INACTIVE TrxState = "INACTIVE"
)

type Transaction struct {
	trxId          TrxId
	state          TrxState
	undoLogRecords []undo.LogRecord
}

func Begin(trxId TrxId) *Transaction {
	return &Transaction{
		trxId:          trxId,
		state:          TRX_ACTIVE,
		undoLogRecords: []undo.LogRecord{},
	}
}

func (t *Transaction) Commit() {
	t.state = TRX_INACTIVE
	t.undoLogRecords = []undo.LogRecord{}
}

func (t *Transaction) Rollback() error {
	// Undo は逆順に実行する
	for i := len(t.undoLogRecords) - 1; i >= 0; i-- {
		if err := t.undoLogRecords[i].Undo(); err != nil {
			return err
		}
	}
	t.state = TRX_INACTIVE
	t.undoLogRecords = []undo.LogRecord{}
	return nil
}

func (t *Transaction) AddUndoLogRecord(logRecord undo.LogRecord) {
	t.undoLogRecords = append(t.undoLogRecords, logRecord)
}

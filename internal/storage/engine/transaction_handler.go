package engine

import "minesql/internal/storage/transaction"

// BeginTrx は新しいトランザクションを開始し、トランザクション ID を返す
func (e *Engine) BeginTrx() TrxId {
	return e.trxManager.Begin()
}

// CommitTrx はトランザクションをコミットする
func (e *Engine) CommitTrx(trxId TrxId) {
	e.trxManager.Commit(trxId)
}

// RollbackTrx はトランザクションをロールバックする
func (e *Engine) RollbackTrx(trxId TrxId) error {
	return e.trxManager.Rollback(e.BufferPool, trxId)
}

// UndoLog は Undo ログを返す
func (e *Engine) UndoLog() *transaction.UndoLog {
	return e.undoLog
}

// AppendInsertUndo は Insert 操作の Undo レコードを記録する
func (e *Engine) AppendInsertUndo(trxId TrxId, table *TableHandler, record [][]byte) {
	e.undoLog.Append(trxId, transaction.NewInsertLogRecord(table, record))
}

// AppendDeleteUndo は Delete 操作の Undo レコードを記録する
func (e *Engine) AppendDeleteUndo(trxId TrxId, table *TableHandler, record [][]byte) {
	e.undoLog.Append(trxId, transaction.NewDeleteLogRecord(table, record))
}

// AppendUpdateInplaceUndo は UpdateInplace 操作の Undo レコードを記録する
func (e *Engine) AppendUpdateInplaceUndo(trxId TrxId, table *TableHandler, prevRecord, newRecord [][]byte) {
	e.undoLog.Append(trxId, transaction.NewUpdateInplaceLogRecord(table, prevRecord, newRecord))
}

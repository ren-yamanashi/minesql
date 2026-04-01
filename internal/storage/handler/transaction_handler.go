package handler

import "minesql/internal/storage/transaction"

// BeginTrx は新しいトランザクションを開始し、トランザクション ID を返す
func (h *Handler) BeginTrx() TrxId {
	return h.trxManager.Begin()
}

// CommitTrx はトランザクションをコミットする
func (h *Handler) CommitTrx(trxId TrxId) {
	h.trxManager.Commit(trxId)
}

// RollbackTrx はトランザクションをロールバックする
func (h *Handler) RollbackTrx(trxId TrxId) error {
	return h.trxManager.Rollback(h.BufferPool, trxId)
}

// UndoLog は Undo ログを返す
func (h *Handler) UndoLog() *transaction.UndoLog {
	return h.undoLog
}

// AppendInsertUndo は Insert 操作の Undo レコードを記録する
func (h *Handler) AppendInsertUndo(trxId TrxId, table *TableHandler, record [][]byte) {
	h.undoLog.Append(trxId, transaction.NewUndoInsertRecord(table.inner, record))
}

// AppendDeleteUndo は Delete 操作の Undo レコードを記録する
func (h *Handler) AppendDeleteUndo(trxId TrxId, table *TableHandler, record [][]byte) {
	h.undoLog.Append(trxId, transaction.NewUndoDeleteRecord(table.inner, record))
}

// AppendUpdateInplaceUndo は UpdateInplace 操作の Undo レコードを記録する
func (h *Handler) AppendUpdateInplaceUndo(trxId TrxId, table *TableHandler, prevRecord, newRecord [][]byte) {
	h.undoLog.Append(trxId, transaction.NewUndoUpdateInplaceRecord(table.inner, prevRecord, newRecord))
}

package handler

import "github.com/ren-yamanashi/minesql/internal/storage/access"

// BeginTrx は新しいトランザクションを開始し、トランザクション ID を返す
func (h *Handler) BeginTrx() TrxId {
	return h.trxManager.Begin()
}

// CommitTrx はトランザクションをコミットする
func (h *Handler) CommitTrx(trxId TrxId) error {
	return h.trxManager.Commit(trxId)
}

// RollbackTrx はトランザクションをロールバックする
func (h *Handler) RollbackTrx(trxId TrxId) error {
	return h.trxManager.Rollback(h.BufferPool, trxId)
}

// CreateReadView は指定したトランザクション用の ReadView を作成する
func (h *Handler) CreateReadView(trxId TrxId) *access.ReadView {
	return h.trxManager.CreateReadView(trxId)
}

// UndoLog は UndoManager を返す
func (h *Handler) UndoLog() *access.UndoManager {
	return h.undoLog
}

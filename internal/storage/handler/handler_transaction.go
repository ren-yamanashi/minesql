package handler

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

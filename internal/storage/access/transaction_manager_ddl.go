package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

// BeginDDL は DDL 用 Transaction を払い出す
//   - DDLReservedTrxId を持つ Transaction オブジェクトを返す
//   - 並行 DDL は想定しないため、 同時に複数の DDL Transaction を保持できない
func (t *TrxManager) BeginDDL() *Transaction {
	return &Transaction{
		trxId: lock.DDLReservedTrxId,
		state: trxStateActive,
		tm:    t,
	}
}

// commitDDL は DDL Transaction の Commit 処理を行う
//   - DDL Undo 領域コンテナの中身をクリアし、 Redo に Commit レコードを追加してフラッシュする
//   - 永続コンテナ型のため、 コンテナ自体 (= root ページ) は残る
func (t *TrxManager) commitDDL(trx *Transaction) error {
	mtr := buffer.NewWriteMtr(t.bufferPool, trx.trxId, t.redoLog)
	if err := t.catalog.DDLManager().Clear(mtr); err != nil {
		mtr.UnpinAll()
		return err
	}
	if err := mtr.Commit(); err != nil {
		return err
	}
	if _, err := t.redoLog.AppendCommit(trx.trxId); err != nil {
		return err
	}
	if err := t.redoLog.Flush(); err != nil {
		return err
	}

	t.mu.Lock()
	trx.state = trxStateInactive
	t.mu.Unlock()
	return nil
}

// rollbackDDL は DDL Transaction の Rollback 処理を行う
//   - DDL Undo 領域コンテナを ReverseScan し、 DDLRollbacker で各 record を逆順適用する
//   - 適用後にコンテナの中身をクリアし、 Redo に Rollback レコードを追加する
//   - 各 record の適用は独立 mtr (= 1 record = 1 mtr) で行い、 record ごとに永続境界を作る
func (t *TrxManager) rollbackDDL(trx *Transaction) error {
	rollbacker := NewDDLRollbacker(t.bufferPool, t.catalog)

	scanMtr := buffer.NewMtr(t.bufferPool)
	defer scanMtr.UnpinAll()
	records, err := t.catalog.DDLManager().ReverseScan(scanMtr)
	if err != nil {
		return err
	}

	for _, record := range records {
		mtr := buffer.NewWriteMtr(t.bufferPool, trx.trxId, t.redoLog)
		if err := rollbacker.Rollback(mtr, record); err != nil {
			mtr.UnpinAll()
			return err
		}
		if err := mtr.Commit(); err != nil {
			return err
		}
	}

	clearMtr := buffer.NewWriteMtr(t.bufferPool, trx.trxId, t.redoLog)
	if err := t.catalog.DDLManager().Clear(clearMtr); err != nil {
		clearMtr.UnpinAll()
		return err
	}
	if err := clearMtr.Commit(); err != nil {
		return err
	}

	if _, err := t.redoLog.AppendRollback(trx.trxId); err != nil {
		return err
	}

	t.mu.Lock()
	trx.state = trxStateInactive
	t.mu.Unlock()
	return nil
}

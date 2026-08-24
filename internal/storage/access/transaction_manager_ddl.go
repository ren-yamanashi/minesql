package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

// BeginDDL は DDL 用 Transaction を払い出す
//   - DDLReservedTrxId を持つ Transaction オブジェクトを返す
//   - 並行 DDL は想定しないため、 同時に複数の DDL Transaction を保持できない
func (t *TrxManager) BeginDDL() *Transaction {
	return &Transaction{
		trxId:      lock.DDLReservedTrxId,
		state:      trxStateActive,
		tm:         t,
		bufferPool: t.bufferPool,
		redoLog:    t.redoLog,
		lockMgr:    t.lock,
		undoLog:    t.undoLog,
		catalog:    t.catalog,
	}
}

// commitDDL は DDL Transaction の Commit 処理を行う
//   - Redo に Commit レコードを追加してフラッシュした後、 DDL Undo 領域コンテナの中身をクリアする
//   - 永続コンテナ型のため、 コンテナ自体 (= root ページ) は残る
//   - Clear が失敗した場合も、 COMMIT レコード到達済みでトランザクションは完了扱いとし、
//     状態を Inactive に遷移した上で Clear の error を返す (残った中身は次回リカバリ完了時にクリアされる)
func (t *TrxManager) commitDDL(trx *Transaction) error {
	if _, err := t.redoLog.AppendCommit(trx.trxId); err != nil {
		return err
	}
	if err := t.redoLog.Flush(); err != nil {
		return err
	}
	clearErr := t.ddlManager.Clear(trx.trxId, t.redoLog)

	t.mu.Lock()
	trx.state = trxStateInactive
	t.mu.Unlock()
	return clearErr
}

// applyDDLRollbackRecord は DDL Undo レコードの取り消しを 1 単位進める
//   - mtr のライフサイクル (Commit / UnpinAll) は呼び出し側が管理する
//   - 通常運用時は書き込み Mtr を、 Recovery 中は読み取り Mtr を渡すことで Redo 記録の有無を切り替える
//   - CreateBTree は step 型のため、呼び出し側は true が返るまで新しい mtr で繰り返す
func (t *TrxManager) applyDDLRollbackRecord(mtr *buffer.Mtr, record undo.DDLRecord) (bool, error) {
	return NewDDLRollbacker(t.bufferPool, t.catalog).Rollback(mtr, record)
}

// rollbackDDL は DDL Transaction の Rollback 処理を行う
//   - DDL Undo 領域コンテナを ReverseScan し、 各 record を逆順適用する
//   - 適用後にコンテナの中身をクリアし、 Redo に Rollback レコードを追加する
//   - 各 record の適用は独立 mtr (= 1 step = 1 mtr) で行い、step ごとに永続境界を作る
func (t *TrxManager) rollbackDDL(trx *Transaction) error {
	scanMtr := trx.NewReadMtr()
	defer scanMtr.UnpinAll()
	records, err := t.ddlManager.ReverseScan(scanMtr)
	if err != nil {
		return err
	}

	for _, record := range records {
		for {
			mtr := trx.NewMtr()
			done, applyErr := t.applyDDLRollbackRecord(mtr, record)
			if commitErr := mtr.Commit(); commitErr != nil && applyErr == nil {
				applyErr = commitErr
			}
			if applyErr != nil {
				return applyErr
			}
			if done {
				break
			}
		}
	}

	if err := t.ddlManager.Clear(trx.trxId, t.redoLog); err != nil {
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

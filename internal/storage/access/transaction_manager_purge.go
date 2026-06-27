package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

// BeginPurge はパージ用 Transaction を払い出す
//   - transactions map には登録しない (= ReadView の活性集合に混入させない)
//   - Purge オブジェクトの NewPurge 内で 1 回だけ呼ぶ規律で運用する
func (t *TrxManager) BeginPurge() *Transaction {
	return &Transaction{
		trxId:      lock.PurgeReservedTrxId,
		state:      trxStateActive,
		tm:         t,
		bufferPool: t.bufferPool,
		redoLog:    t.redoLog,
		lockMgr:    t.lock,
		undoLog:    t.undoLog,
		catalog:    t.catalog,
	}
}

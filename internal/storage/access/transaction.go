package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
)

type trxState int

const (
	trxStateActive trxState = iota + 1
	trxStateInactive
)

// Transaction はトランザクションの状態と参照するストレージリソースを保持する
//   - trxId: トランザクション ID (Begin 時に払い出される)
//   - state: Active / Inactive
//   - readView: 最初の Consistent Read 時に作成。それまで nil
//   - tm: 自分を生成した TrxManager への参照
//   - bufferPool / redoLog / lockMgr / undoLog / catalog: トランザクションが操作する各リソース
type Transaction struct {
	trxId      lock.TrxId
	state      trxState
	readView   *readView
	tm         *TrxManager
	bufferPool *buffer.Pool
	redoLog    *redo.Buffer
	lockMgr    *lock.Manager
	undoLog    *undo.Manager
	catalog    *dictionary.Catalog
}

// NewMtr は書き込み用の mtr を払い出す
func (t *Transaction) NewMtr() *buffer.Mtr {
	return buffer.NewWriteMtr(t.bufferPool, t.trxId, t.redoLog)
}

// NewReadMtr は読み取り用の mtr を払い出す
func (t *Transaction) NewReadMtr() *buffer.Mtr {
	return buffer.NewMtr(t.bufferPool)
}

// DDLManager は Transaction が属する TrxManager の ddlManager を返す
//   - DDL 経路 (= BeginDDL で払い出された Transaction) からの利用を想定
func (t *Transaction) DDLManager() *undo.DDLManager {
	return t.tm.ddlManager
}

// Savepoint は現時点の Undo 位置を返す
//   - 呼び出し側は文の開始時に取得し、 その文が失敗した場合に RollbackToSavepoint へ渡す
func (t *Transaction) Savepoint() int {
	return t.undoLog.Count(t.trxId)
}

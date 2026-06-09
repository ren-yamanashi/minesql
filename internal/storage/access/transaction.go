package access

import (
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type trxState int

const (
	trxStateActive trxState = iota + 1
	trxStateInactive
)

// Transaction はトランザクションの状態を保持する
//   - trxId: トランザクション ID (Begin 時に払い出される)
//   - state: Active / Inactive
//   - readView: 最初の Consistent Read 時に作成。それまで nil
//   - tm: 自分を生成した TrxManager への参照
type Transaction struct {
	trxId    lock.TrxId
	state    trxState
	readView *readView
	tm       *TrxManager
}

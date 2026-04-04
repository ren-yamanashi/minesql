package transaction

type lockMode int

const (
	sharedMode    lockMode = iota // 共有ロック
	exclusiveMode                 // 排他ロック
)

// LockRequest はロックの要求を表す
type LockRequest struct {
	trxId TrxId
	mode  lockMode
}

// LockState は特定の行に対するロックの状態を管理する構造体
type LockState struct {
	handlers  map[TrxId]lockMode // 現在のロック保持者とそのロックモードを管理するマップ
	waitQueue []*LockRequest     // ロックを待機しているトランザクションの待機キュー
}

func NewLockState(trxId TrxId, mode lockMode) *LockState {
	req := &LockRequest{
		trxId: trxId,
		mode:  mode,
	}
	return &LockState{
		handlers:  map[TrxId]lockMode{TrxId(trxId): mode},
		waitQueue: []*LockRequest{req},
	}
}

// IsCompatible は指定したロックモードが、現在のロック保持者と競合しないかを判断する
func (ls *LockState) IsCompatible(mode lockMode) bool {
	// ロックが存在しない場合は常に競合しない
	if len(ls.handlers) == 0 {
		return true
	}

	switch mode {
	case sharedMode:
		// 現在の保持者がすべて共有ロックであれば競合しない
		for _, m := range ls.handlers {
			if m != sharedMode {
				return false
			}
		}
		return true
	case exclusiveMode:
		// 排他ロックは常に競合する
		return false
	default:
		// 実際にはここには到達しない
		return false
	}
}

// CanGrant は指定したトランザクション ID に対してロックを付与できるかを判断する
func (ls *LockState) CanGrant(trxId TrxId, mode lockMode) bool {
	// すでにロックを保持している場合
	if m, exists := ls.handlers[trxId]; exists {
		// 同じモードのロックを要求している場合は許可される
		if m == mode {
			return true
		}
		// 共有ロックから排他ロックへ昇格したい場合は、他の保持者がいないことが条件となる
		if m == sharedMode && mode == exclusiveMode {
			return len(ls.handlers) == 1
		}
		// その他のケースは許可されない
		return false
	}

	// ロックを保持していない場合
	// 競合がなく、かつ待ち行列が空であれば許可される
	return ls.IsCompatible(mode) && len(ls.waitQueue) == 0
}

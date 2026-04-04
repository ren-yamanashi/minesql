package lock

// TrxId はトランザクション ID
type TrxId = uint64

// LockMode はロックの種類を表す
type LockMode int

const (
	Shared    LockMode = iota // 共有ロック (読み取り用)
	Exclusive                 // 排他ロック (書き込み用)
)

// lockRequest はロックの要求を表す
type lockRequest struct {
	trxId TrxId
	mode  LockMode
}

// lockState は特定の行に対するロックの状態を管理する構造体
type lockState struct {
	holders   map[TrxId]LockMode // 現在のロック保持者とそのロックモードを管理するマップ
	waitQueue []*lockRequest     // ロックを待機しているトランザクションの待機キュー
}

func newLockState() *lockState {
	return &lockState{
		holders: make(map[TrxId]LockMode),
	}
}

// isCompatible は指定したロックモードが、現在のロック保持者と競合しないかを判断する
func (ls *lockState) isCompatible(mode LockMode) bool {
	// ロックが存在しない場合は常に競合しない
	if len(ls.holders) == 0 {
		return true
	}

	switch mode {
	case Shared:
		// 現在の保持者がすべて共有ロックであれば競合しない
		for _, m := range ls.holders {
			if m != Shared {
				return false
			}
		}
		return true
	case Exclusive:
		// 排他ロックは常に競合する
		return false
	default:
		// 実際にはここには到達しない
		return false
	}
}

// canGrant は指定したトランザクション ID に対してロックを付与できるかを判断する
func (ls *lockState) canGrant(trxId TrxId, mode LockMode) bool {
	// すでにロックを保持している場合
	if m, exists := ls.holders[trxId]; exists {
		// 同じモードのロックを要求している場合は許可される
		if m == mode {
			return true
		}
		// 共有ロックから排他ロックへ昇格したい場合は、他の保持者がいないことが条件となる
		if m == Shared && mode == Exclusive {
			return len(ls.holders) == 1
		}
		// その他のケースは許可されない
		return false
	}

	// ロックを保持していない場合
	// 競合がなく、かつ待ち行列が空であれば許可される
	return ls.isCompatible(mode) && len(ls.waitQueue) == 0
}

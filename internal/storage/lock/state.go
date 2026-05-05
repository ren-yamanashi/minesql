package lock

type (
	TrxId = uint32
	Mode  = int
)

const (
	Shared    Mode = iota + 1 // 共有ロック
	Exclusive                 // 排他ロック
)

type request struct {
	trxId TrxId
	mode  Mode
}

// state は特定のレコードのロック状態
type state struct {
	holders   map[TrxId]Mode // 現在のロック保持者 → ロックモードのマップ
	waitQueue []*request     // ロックを待機しているトランザクションの待機キュー
}

func newState() *state {
	return &state{
		holders: make(map[TrxId]Mode),
	}
}

// isCompatible は指定したロックモードが現在のロック保持者と競合しないかを判定する
func (s *state) isCompatible(mode Mode) bool {
	if len(s.holders) == 0 {
		return true
	}

	switch mode {
	case Shared:
		// 現在の保持者がすべて Shared であれば競合しない
		for _, m := range s.holders {
			if m != Shared {
				return false
			}
		}
		return true
	case Exclusive:
		return false
	default:
		return false
	}
}

// canGrant は指定したトランザクション ID に対してロックを付与できるか判定する
func (s *state) canGrant(trxId TrxId, mode Mode) bool {
	m, exists := s.holders[trxId]
	// ロックを保持していない場合
	if !exists {
		return s.isCompatible(mode) && len(s.waitQueue) == 0
	}

	// 既にロックを保持している場合
	if m == mode {
		return true
	}
	// Shared -> Exclusive の昇格は他の保持者がいなければ可能
	if m == Shared && mode == Exclusive {
		return len(s.holders) == 1
	}
	return false
}

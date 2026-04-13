package access

import "slices"

// ReadView はトランザクションの可視性判定に使用するスナップショット
type ReadView struct {
	TrxId       TrxId   // 自分のトランザクション ID
	MUpLimitId  TrxId   // アクティブトランザクションの最小 trxId (これ未満は確実にコミット済みで可視)
	MLowLimitId TrxId   // 次に払い出される trxId (これ以上は不可視)
	MIds        []TrxId // ReadView 作成時点でアクティブ (未コミット) なトランザクション ID のリスト
}

// NewReadView は ReadView を作成する
//   - trxId: 自分のトランザクション ID
//   - activeTrxIds: ReadView 作成時点でアクティブなトランザクション ID のリスト (自分自身を含まない)
//   - nextTrxId: 次に払い出されるトランザクション ID
func NewReadView(trxId TrxId, activeTrxIds []TrxId, nextTrxId TrxId) *ReadView {
	// mUpLimitId を算出 (activeTrxIds の中で最小の ID を選ぶ)
	mUpLimitId := nextTrxId
	for _, id := range activeTrxIds {
		if id < mUpLimitId {
			mUpLimitId = id
		}
	}

	return &ReadView{
		TrxId:       trxId,
		MUpLimitId:  mUpLimitId,
		MLowLimitId: nextTrxId,
		MIds:        activeTrxIds,
	}
}

// IsVisible は指定された trxId のレコードが可視かどうかを判定する
func (rv *ReadView) IsVisible(recordTrxId TrxId) bool {
	// 自分の変更は常に可視
	if recordTrxId == rv.TrxId {
		return true
	}

	// ReadView 作成前にコミット済み
	if recordTrxId < rv.MUpLimitId {
		return true
	}

	// ReadView 作成後に開始されたトランザクション
	if recordTrxId >= rv.MLowLimitId {
		return false
	}

	// MUpLimitId <= recordTrxId < MLowLimitId の範囲: MIds を確認
	// MIds に含まれていれば不可視 (作成時点で実行中だった)、含まれていなければ可視 (コミット済みだった)
	return !slices.Contains(rv.MIds, recordTrxId)
}

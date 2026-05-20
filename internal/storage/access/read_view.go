package access

import (
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

// ReadView はトランザクションの可視性判定に使用するスナップショット
type ReadView struct {
	TrxId        lock.TrxId   // 自身の TrxId
	MUpLimitId   lock.TrxId   // アクティブトランザクションの最小の TrxId (これ未満は確実にコミット済みで可視)
	MLockLimitId lock.TrxId   // 次に払い出される TrxId (これ以上は不可視)
	MIds         []lock.TrxId // ReadView 作成時点でアクティブ (未コミット) な TrxId 一覧
}

func NewReadView(trxId lock.TrxId, mdIds []lock.TrxId, nextTrxId lock.TrxId) *ReadView {
	mUpLimitId := nextTrxId
	for _, id := range mdIds {
		if id < mUpLimitId {
			mUpLimitId = id
		}
	}
	return &ReadView{
		TrxId:        trxId,
		MUpLimitId:   mUpLimitId,
		MLockLimitId: nextTrxId,
		MIds:         mdIds,
	}
}

// IsVisible は指定された trxId のレコードが可視かどうか判定する
func (rv *ReadView) IsVisible(recordTrxId lock.TrxId) bool {
	if recordTrxId == rv.TrxId {
		return true
	}
	if recordTrxId < rv.MUpLimitId {
		return true
	}
	if recordTrxId >= rv.MLockLimitId {
		return false
	}
	return !slices.Contains(rv.MIds, recordTrxId)
}

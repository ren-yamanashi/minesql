package access

import (
	"slices"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
)

type readView struct {
	trxId        lock.TrxId   // 自身の TrxId
	upLimitId    lock.TrxId   // アクティブトランザクションの最小の TrxId (これ未満は確実にコミット済みで可視)
	lowLimitId   lock.TrxId   // 次に払い出される TrxId (これ以上は不可視)
	activeTrxIds []lock.TrxId // ReadView 作成時点でアクティブ (未コミット) な TrxId 一覧
}

func newReadView(trxId lock.TrxId, activeTrxIds []lock.TrxId, nextTrxId lock.TrxId) *readView {
	upLimitId := nextTrxId
	if len(activeTrxIds) > 0 {
		upLimitId = min(nextTrxId, slices.Min(activeTrxIds))
	}
	return &readView{
		trxId:        trxId,
		upLimitId:    upLimitId,
		lowLimitId:   nextTrxId,
		activeTrxIds: activeTrxIds,
	}
}

func (rv *readView) isVisible(recordTrxId lock.TrxId) bool {
	if recordTrxId == rv.trxId {
		return true
	}
	if recordTrxId < rv.upLimitId {
		return true
	}
	if recordTrxId >= rv.lowLimitId {
		return false
	}
	return !slices.Contains(rv.activeTrxIds, recordTrxId)
}

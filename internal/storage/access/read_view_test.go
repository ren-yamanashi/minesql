package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

func TestNewReadView(t *testing.T) {
	t.Run("アクティブトランザクションの最小値が upLimitId になる", func(t *testing.T) {
		// GIVEN
		activeTrxIds := []lock.TrxId{5, 3, 7}

		// WHEN
		rv := newReadView(10, activeTrxIds, 11)

		// THEN
		assert.Equal(t, lock.TrxId(10), rv.trxId)
		assert.Equal(t, lock.TrxId(3), rv.upLimitId)
		assert.Equal(t, lock.TrxId(11), rv.lowLimitId)
		assert.Equal(t, activeTrxIds, rv.activeTrxIds)
	})

	t.Run("アクティブトランザクションが空の場合 upLimitId は nextTrxId になる", func(t *testing.T) {
		// GIVEN
		activeTrxIds := []lock.TrxId{}

		// WHEN
		rv := newReadView(10, activeTrxIds, 11)

		// THEN
		assert.Equal(t, lock.TrxId(11), rv.upLimitId)
		assert.Equal(t, lock.TrxId(11), rv.lowLimitId)
	})

	t.Run("アクティブトランザクションが 1 つの場合", func(t *testing.T) {
		// GIVEN
		activeTrxIds := []lock.TrxId{5}

		// WHEN
		rv := newReadView(10, activeTrxIds, 11)

		// THEN
		assert.Equal(t, lock.TrxId(5), rv.upLimitId)
	})
}

func TestIsVisible(t *testing.T) {
	// 共通の ReadView: trxId=10, アクティブ=[5,7], nextTrxId=11
	// upLimitId=5, lowLimitId=11
	rv := newReadView(10, []lock.TrxId{5, 7}, 11)

	t.Run("自分自身の変更は可視", func(t *testing.T) {
		// WHEN
		result := rv.isVisible(10)

		// THEN
		assert.True(t, result)
	})

	t.Run("upLimitId 未満の trxId は可視 (確実にコミット済み)", func(t *testing.T) {
		// WHEN
		result := rv.isVisible(4)

		// THEN
		assert.True(t, result)
	})

	t.Run("upLimitId ちょうどの trxId はアクティブなので不可視", func(t *testing.T) {
		// WHEN: upLimitId=5 で activeTrxIds に 5 が含まれる
		result := rv.isVisible(5)

		// THEN
		assert.False(t, result)
	})

	t.Run("lowLimitId 以上の trxId は不可視 (ReadView 作成後に開始)", func(t *testing.T) {
		// WHEN
		result := rv.isVisible(11)

		// THEN
		assert.False(t, result)
	})

	t.Run("lowLimitId より大きい trxId も不可視", func(t *testing.T) {
		// WHEN
		result := rv.isVisible(100)

		// THEN
		assert.False(t, result)
	})

	t.Run("upLimitId と lowLimitId の間で activeTrxIds に含まれる trxId は不可視", func(t *testing.T) {
		// WHEN: trxId=7 は activeTrxIds に含まれる
		result := rv.isVisible(7)

		// THEN
		assert.False(t, result)
	})

	t.Run("upLimitId と lowLimitId の間で activeTrxIds に含まれない trxId は可視 (コミット済み)", func(t *testing.T) {
		// WHEN: trxId=6 は activeTrxIds に含まれない
		result := rv.isVisible(6)

		// THEN
		assert.True(t, result)
	})

	t.Run("trxId=0 は可視", func(t *testing.T) {
		// WHEN
		result := rv.isVisible(0)

		// THEN
		assert.True(t, result)
	})

	t.Run("アクティブトランザクションが空の ReadView では upLimitId 未満がすべて可視", func(t *testing.T) {
		// GIVEN: アクティブなし, nextTrxId=10 → upLimitId=10, lowLimitId=10
		emptyRv := newReadView(10, []lock.TrxId{}, 10)

		// WHEN/THEN
		assert.True(t, emptyRv.isVisible(10))  // 自分自身
		assert.True(t, emptyRv.isVisible(9))   // upLimitId 未満
		assert.False(t, emptyRv.isVisible(11)) // lowLimitId 以上
	})
}

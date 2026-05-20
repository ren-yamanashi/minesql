package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

func TestNewReadView(t *testing.T) {
	t.Run("アクティブトランザクションの最小値が MUpLimitId になる", func(t *testing.T) {
		// GIVEN
		mIds := []lock.TrxId{5, 3, 7}

		// WHEN
		rv := NewReadView(10, mIds, 11)

		// THEN
		assert.Equal(t, lock.TrxId(10), rv.TrxId)
		assert.Equal(t, lock.TrxId(3), rv.MUpLimitId)
		assert.Equal(t, lock.TrxId(11), rv.MLockLimitId)
		assert.Equal(t, mIds, rv.MIds)
	})

	t.Run("アクティブトランザクションが空の場合 MUpLimitId は nextTrxId になる", func(t *testing.T) {
		// GIVEN
		mIds := []lock.TrxId{}

		// WHEN
		rv := NewReadView(10, mIds, 11)

		// THEN
		assert.Equal(t, lock.TrxId(11), rv.MUpLimitId)
		assert.Equal(t, lock.TrxId(11), rv.MLockLimitId)
	})

	t.Run("アクティブトランザクションが 1 つの場合", func(t *testing.T) {
		// GIVEN
		mIds := []lock.TrxId{5}

		// WHEN
		rv := NewReadView(10, mIds, 11)

		// THEN
		assert.Equal(t, lock.TrxId(5), rv.MUpLimitId)
	})
}

func TestIsVisible(t *testing.T) {
	// 共通の ReadView: trxId=10, アクティブ=[5,7], nextTrxId=11
	// MUpLimitId=5, MLockLimitId=11
	rv := NewReadView(10, []lock.TrxId{5, 7}, 11)

	t.Run("自分自身の変更は可視", func(t *testing.T) {
		// WHEN
		result := rv.IsVisible(10)

		// THEN
		assert.True(t, result)
	})

	t.Run("MUpLimitId 未満の trxId は可視 (確実にコミット済み)", func(t *testing.T) {
		// WHEN
		result := rv.IsVisible(4)

		// THEN
		assert.True(t, result)
	})

	t.Run("MUpLimitId ちょうどの trxId はアクティブなので不可視", func(t *testing.T) {
		// WHEN: MUpLimitId=5 で mIds に 5 が含まれる
		result := rv.IsVisible(5)

		// THEN
		assert.False(t, result)
	})

	t.Run("MLockLimitId 以上の trxId は不可視 (ReadView 作成後に開始)", func(t *testing.T) {
		// WHEN
		result := rv.IsVisible(11)

		// THEN
		assert.False(t, result)
	})

	t.Run("MLockLimitId より大きい trxId も不可視", func(t *testing.T) {
		// WHEN
		result := rv.IsVisible(100)

		// THEN
		assert.False(t, result)
	})

	t.Run("MUpLimitId と MLockLimitId の間で mIds に含まれる trxId は不可視", func(t *testing.T) {
		// WHEN: trxId=7 は mIds に含まれる
		result := rv.IsVisible(7)

		// THEN
		assert.False(t, result)
	})

	t.Run("MUpLimitId と MLockLimitId の間で mIds に含まれない trxId は可視 (コミット済み)", func(t *testing.T) {
		// WHEN: trxId=6 は mIds に含まれない
		result := rv.IsVisible(6)

		// THEN
		assert.True(t, result)
	})

	t.Run("trxId=0 は可視", func(t *testing.T) {
		// WHEN
		result := rv.IsVisible(0)

		// THEN
		assert.True(t, result)
	})

	t.Run("アクティブトランザクションが空の ReadView では MUpLimitId 未満がすべて可視", func(t *testing.T) {
		// GIVEN: アクティブなし, nextTrxId=10 → MUpLimitId=10, MLockLimitId=10
		emptyRv := NewReadView(10, []lock.TrxId{}, 10)

		// WHEN/THEN
		assert.True(t, emptyRv.IsVisible(10))  // 自分自身
		assert.True(t, emptyRv.IsVisible(9))   // MUpLimitId 未満
		assert.False(t, emptyRv.IsVisible(11)) // MLockLimitId 以上
	})
}

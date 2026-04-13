package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewReadView(t *testing.T) {
	t.Run("アクティブトランザクションがない場合 MUpLimitId は nextTrxId と同じ", func(t *testing.T) {
		// GIVEN / WHEN
		rv := NewReadView(5, nil, 6)

		// THEN
		assert.Equal(t, TrxId(5), rv.TrxId)
		assert.Equal(t, TrxId(6), rv.MUpLimitId)
		assert.Equal(t, TrxId(6), rv.MLowLimitId)
	})

	t.Run("アクティブトランザクションの最小値が MUpLimitId になる", func(t *testing.T) {
		// GIVEN / WHEN
		rv := NewReadView(5, []TrxId{3, 7, 4}, 10)

		// THEN
		assert.Equal(t, TrxId(3), rv.MUpLimitId)
		assert.Equal(t, TrxId(10), rv.MLowLimitId)
	})
}

func TestReadViewIsVisible(t *testing.T) {
	// テスト用の ReadView:
	//   自分: trxId=5
	//   アクティブ: [3, 7]
	//   MUpLimitId=3, MLowLimitId=10
	//
	//   trxId: 1  2  [3]  4  (5)  6  [7]  8  9  | 10  11 ...
	//          可視     不可視  自分  可視     不可視        不可視
	rv := NewReadView(5, []TrxId{3, 7}, 10)

	t.Run("自分の変更は可視", func(t *testing.T) {
		// WHEN / THEN
		assert.True(t, rv.IsVisible(5))
	})

	t.Run("MUpLimitId 未満はコミット済みで可視", func(t *testing.T) {
		// WHEN / THEN
		assert.True(t, rv.IsVisible(1))
		assert.True(t, rv.IsVisible(2))
	})

	t.Run("MLowLimitId 以上は不可視", func(t *testing.T) {
		// WHEN / THEN
		assert.False(t, rv.IsVisible(10))
		assert.False(t, rv.IsVisible(11))
	})

	t.Run("MIds に含まれるトランザクションは不可視", func(t *testing.T) {
		// WHEN / THEN
		assert.False(t, rv.IsVisible(3))
		assert.False(t, rv.IsVisible(7))
	})

	t.Run("MUpLimitId 以上 MLowLimitId 未満で MIds に含まれなければ可視", func(t *testing.T) {
		// WHEN / THEN
		assert.True(t, rv.IsVisible(4))
		assert.True(t, rv.IsVisible(6))
		assert.True(t, rv.IsVisible(8))
		assert.True(t, rv.IsVisible(9))
	})
}

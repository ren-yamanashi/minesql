package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsCompatible(t *testing.T) {
	t.Run("保持者がいない場合、どのモードでも互換性がある", func(t *testing.T) {
		// GIVEN
		state := &LockState{handlers: map[TrxId]lockMode{}}

		// WHEN / THEN
		assert.True(t, state.IsCompatible(sharedMode))
		assert.True(t, state.IsCompatible(exclusiveMode))
	})

	t.Run("共有ロックのみ保持されている場合、共有ロックは互換性がある", func(t *testing.T) {
		// GIVEN
		state := &LockState{handlers: map[TrxId]lockMode{1: sharedMode}}

		// WHEN / THEN
		assert.True(t, state.IsCompatible(sharedMode))
	})

	t.Run("共有ロックのみ保持されている場合、排他ロックは互換性がない", func(t *testing.T) {
		// GIVEN
		state := &LockState{handlers: map[TrxId]lockMode{1: sharedMode}}

		// WHEN / THEN
		assert.False(t, state.IsCompatible(exclusiveMode))
	})

	t.Run("排他ロックが保持されている場合、どのモードでも互換性がない", func(t *testing.T) {
		// GIVEN
		state := &LockState{handlers: map[TrxId]lockMode{1: exclusiveMode}}

		// WHEN / THEN
		assert.False(t, state.IsCompatible(sharedMode))
		assert.False(t, state.IsCompatible(exclusiveMode))
	})

	t.Run("複数の共有ロックが保持されている場合、共有ロックは互換性がある", func(t *testing.T) {
		// GIVEN
		state := &LockState{handlers: map[TrxId]lockMode{1: sharedMode, 2: sharedMode}}

		// WHEN / THEN
		assert.True(t, state.IsCompatible(sharedMode))
	})
}

func TestCanGrant(t *testing.T) {
	t.Run("ロック未保持で競合なし・待機キュー空の場合、付与可能", func(t *testing.T) {
		// GIVEN
		state := &LockState{
			handlers:  map[TrxId]lockMode{},
			waitQueue: nil,
		}

		// WHEN / THEN
		assert.True(t, state.CanGrant(1, sharedMode))
		assert.True(t, state.CanGrant(1, exclusiveMode))
	})

	t.Run("同じモードのロックを既に保持している場合、付与可能", func(t *testing.T) {
		// GIVEN
		state := &LockState{
			handlers:  map[TrxId]lockMode{1: sharedMode},
			waitQueue: nil,
		}

		// WHEN / THEN
		assert.True(t, state.CanGrant(1, sharedMode))
	})

	t.Run("排他ロック保持中の共有要求は CanGrant では false (Lock 側で早期リターンされる)", func(t *testing.T) {
		// GIVEN: CanGrant は「同じモードか昇格」のみ許可する
		// 排他→共有のダウングレードは Lock メソッドの早期リターンで処理される
		state := &LockState{
			handlers:  map[TrxId]lockMode{1: exclusiveMode},
			waitQueue: nil,
		}

		// WHEN / THEN
		assert.False(t, state.CanGrant(1, sharedMode))
	})

	t.Run("共有→排他への昇格は自身が唯一の保持者の場合のみ可能", func(t *testing.T) {
		// GIVEN: trx1 のみが共有ロックを保持
		state := &LockState{
			handlers:  map[TrxId]lockMode{1: sharedMode},
			waitQueue: nil,
		}

		// WHEN / THEN
		assert.True(t, state.CanGrant(1, exclusiveMode))
	})

	t.Run("共有→排他への昇格は他の保持者がいる場合は不可", func(t *testing.T) {
		// GIVEN: trx1 と trx2 が共有ロックを保持
		state := &LockState{
			handlers:  map[TrxId]lockMode{1: sharedMode, 2: sharedMode},
			waitQueue: nil,
		}

		// WHEN / THEN
		assert.False(t, state.CanGrant(1, exclusiveMode))
	})

	t.Run("待機キューが空でない場合、新規ロックは付与不可 (FIFO 保証)", func(t *testing.T) {
		// GIVEN: 互換性はあるが待機キューに先客がいる
		state := &LockState{
			handlers:  map[TrxId]lockMode{},
			waitQueue: []*LockRequest{{trxId: 2, mode: exclusiveMode}},
		}

		// WHEN / THEN
		assert.False(t, state.CanGrant(1, sharedMode))
	})

	t.Run("他のトランザクションが排他ロックを保持している場合、付与不可", func(t *testing.T) {
		// GIVEN
		state := &LockState{
			handlers:  map[TrxId]lockMode{1: exclusiveMode},
			waitQueue: nil,
		}

		// WHEN / THEN
		assert.False(t, state.CanGrant(2, sharedMode))
		assert.False(t, state.CanGrant(2, exclusiveMode))
	})
}

package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLockState(t *testing.T) {
	t.Run("空の lockState が生成される", func(t *testing.T) {
		// GIVEN / WHEN
		ls := newLockState()

		// THEN
		assert.NotNil(t, ls.holders)
		assert.Equal(t, 0, len(ls.holders))
		assert.Nil(t, ls.waitQueue)
	})
}

func TestIsCompatible(t *testing.T) {
	t.Run("ホルダーがいない場合_共有ロックは互換性がある", func(t *testing.T) {
		// GIVEN
		ls := newLockState()

		// WHEN
		result := ls.isCompatible(Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("ホルダーがいない場合_排他ロックは互換性がある", func(t *testing.T) {
		// GIVEN
		ls := newLockState()

		// WHEN
		result := ls.isCompatible(Exclusive)

		// THEN
		assert.True(t, result)
	})

	t.Run("共有ロックが保持されている場合_共有ロックは互換性がある", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Shared}}

		// WHEN
		result := ls.isCompatible(Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("複数の共有ロックが保持されている場合_共有ロックは互換性がある", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Shared, 2: Shared}}

		// WHEN
		result := ls.isCompatible(Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("共有ロックが保持されている場合_排他ロックは互換性がない", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Shared}}

		// WHEN
		result := ls.isCompatible(Exclusive)

		// THEN
		assert.False(t, result)
	})

	t.Run("排他ロックが保持されている場合_共有ロックは互換性がない", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Exclusive}}

		// WHEN
		result := ls.isCompatible(Shared)

		// THEN
		assert.False(t, result)
	})

	t.Run("排他ロックが保持されている場合_排他ロックは互換性がない", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Exclusive}}

		// WHEN
		result := ls.isCompatible(Exclusive)

		// THEN
		assert.False(t, result)
	})
}

func TestCanGrant(t *testing.T) {
	t.Run("ホルダーがいない場合_共有ロックを付与できる", func(t *testing.T) {
		// GIVEN
		ls := newLockState()

		// WHEN
		result := ls.canGrant(1, Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("ホルダーがいない場合_排他ロックを付与できる", func(t *testing.T) {
		// GIVEN
		ls := newLockState()

		// WHEN
		result := ls.canGrant(1, Exclusive)

		// THEN
		assert.True(t, result)
	})

	t.Run("同一トランザクションが共有ロックを保持している場合_共有ロックを付与できる", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Shared}}

		// WHEN
		result := ls.canGrant(1, Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("同一トランザクションが排他ロックを保持している場合_排他ロックを付与できる", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Exclusive}}

		// WHEN
		result := ls.canGrant(1, Exclusive)

		// THEN
		assert.True(t, result)
	})

	t.Run("同一トランザクションが排他ロックを保持している場合_共有ロックのダウングレードは canGrant では許可されない", func(t *testing.T) {
		// GIVEN
		// 注: Lock() メソッドでは排他ロック保持中の共有ロック要求は canGrant を呼ばずに早期リターンする
		ls := &lockState{holders: map[TrxId]LockMode{1: Exclusive}}

		// WHEN
		result := ls.canGrant(1, Shared)

		// THEN
		assert.False(t, result)
	})

	t.Run("同一トランザクションのみが共有ロックを保持している場合_排他ロックへアップグレードできる", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Shared}}

		// WHEN
		result := ls.canGrant(1, Exclusive)

		// THEN
		assert.True(t, result)
	})

	t.Run("他のトランザクションも共有ロックを保持している場合_排他ロックへアップグレードできない", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Shared, 2: Shared}}

		// WHEN
		result := ls.canGrant(1, Exclusive)

		// THEN
		assert.False(t, result)
	})

	t.Run("他のトランザクションが共有ロックを保持している場合_共有ロックを付与できる", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Shared}}

		// WHEN
		result := ls.canGrant(2, Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("他のトランザクションが排他ロックを保持している場合_共有ロックを付与できない", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Exclusive}}

		// WHEN
		result := ls.canGrant(2, Shared)

		// THEN
		assert.False(t, result)
	})

	t.Run("他のトランザクションが排他ロックを保持している場合_排他ロックを付与できない", func(t *testing.T) {
		// GIVEN
		ls := &lockState{holders: map[TrxId]LockMode{1: Exclusive}}

		// WHEN
		result := ls.canGrant(2, Exclusive)

		// THEN
		assert.False(t, result)
	})

	t.Run("待機キューが存在する場合_新しいトランザクションにはロックを付与しない", func(t *testing.T) {
		// GIVEN
		ls := &lockState{
			holders:   map[TrxId]LockMode{1: Shared},
			waitQueue: []*lockRequest{{trxId: 3, mode: Exclusive}},
		}

		// WHEN
		result := ls.canGrant(2, Shared)

		// THEN
		assert.False(t, result)
	})

	t.Run("待機キューが存在しても_既にロックを保持しているトランザクションは付与できる", func(t *testing.T) {
		// GIVEN
		ls := &lockState{
			holders:   map[TrxId]LockMode{1: Shared},
			waitQueue: []*lockRequest{{trxId: 3, mode: Exclusive}},
		}

		// WHEN
		result := ls.canGrant(1, Shared)

		// THEN
		assert.True(t, result)
	})
}

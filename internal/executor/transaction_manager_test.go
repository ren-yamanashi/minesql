package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTrxManager(t *testing.T) {
	t.Run("空の TrxManager が生成される", func(t *testing.T) {
		manager := NewTrxManager()

		assert.NotNil(t, manager)
		assert.Equal(t, 0, len(manager.transactions))
	})
}

func TestAllocateTrxId(t *testing.T) {
	t.Run("トランザクションが存在しない場合は TrxId 1 が割り当てられる", func(t *testing.T) {
		manager := NewTrxManager()

		id := manager.AllocateTrxId()

		assert.Equal(t, TrxId(1), id)
	})

	t.Run("既存のトランザクションの最大 ID + 1 が割り当てられる", func(t *testing.T) {
		manager := NewTrxManager()
		manager.transactions[TrxId(1)] = Begin(1)
		manager.transactions[TrxId(3)] = Begin(3)

		id := manager.AllocateTrxId()

		assert.Equal(t, TrxId(4), id)
	})

	t.Run("連続して割り当てると単調増加する", func(t *testing.T) {
		manager := NewTrxManager()

		id1 := manager.AllocateTrxId()
		manager.transactions[id1] = Begin(id1)

		id2 := manager.AllocateTrxId()
		manager.transactions[id2] = Begin(id2)

		id3 := manager.AllocateTrxId()

		assert.Equal(t, TrxId(1), id1)
		assert.Equal(t, TrxId(2), id2)
		assert.Equal(t, TrxId(3), id3)
	})
}

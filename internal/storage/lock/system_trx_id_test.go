package lock

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSystemReservedTrxId(t *testing.T) {
	t.Run("ユーザー採番と衝突しない値が予約されている", func(t *testing.T) {
		// GIVEN
		userInitialTrxId := TrxId(1)
		unassignedTrxId := TrxId(0)

		// WHEN
		reserved := SystemReservedTrxId

		// THEN
		assert.Equal(t, TrxId(math.MaxUint32), reserved)
		assert.NotEqual(t, unassignedTrxId, reserved)
		assert.NotEqual(t, userInitialTrxId, reserved)
	})
}

func TestPurgeReservedTrxId(t *testing.T) {
	t.Run("ユーザー採番および SystemReservedTrxId と衝突しない値が予約されている", func(t *testing.T) {
		// GIVEN
		userInitialTrxId := TrxId(1)
		unassignedTrxId := TrxId(0)

		// WHEN
		reserved := PurgeReservedTrxId

		// THEN
		assert.Equal(t, TrxId(math.MaxUint32-1), reserved)
		assert.NotEqual(t, unassignedTrxId, reserved)
		assert.NotEqual(t, userInitialTrxId, reserved)
		assert.NotEqual(t, SystemReservedTrxId, reserved)
	})
}

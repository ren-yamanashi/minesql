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

package lock

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDDLReservedTrxId(t *testing.T) {
	t.Run("ユーザー採番および他の予約 ID と衝突しない値が予約されている", func(t *testing.T) {
		// GIVEN
		userInitialTrxId := TrxId(1)
		unassignedTrxId := TrxId(0)

		// WHEN
		reserved := DDLReservedTrxId

		// THEN
		assert.Equal(t, TrxId(math.MaxUint32-2), reserved)
		assert.NotEqual(t, unassignedTrxId, reserved)
		assert.NotEqual(t, userInitialTrxId, reserved)
		assert.NotEqual(t, SystemReservedTrxId, reserved)
		assert.NotEqual(t, PurgeReservedTrxId, reserved)
	})

	t.Run("3 つの予約 ID が互いにユニークかつ既存値が retention されている", func(t *testing.T) {
		// GIVEN
		system := SystemReservedTrxId
		purge := PurgeReservedTrxId
		ddl := DDLReservedTrxId

		// WHEN
		reserved := map[TrxId]struct{}{
			system: {},
			purge:  {},
			ddl:    {},
		}

		// THEN
		assert.Len(t, reserved, 3)
		assert.Equal(t, TrxId(math.MaxUint32), system)
		assert.Equal(t, TrxId(math.MaxUint32-1), purge)
		assert.Equal(t, TrxId(math.MaxUint32-2), ddl)
	})
}

package lock

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSystemReservedTrxId(t *testing.T) {
	t.Run("予約値は TrxId 型の最大値である", func(t *testing.T) {
		// GIVEN
		// WHEN
		// THEN
		assert.Equal(t, TrxId(math.MaxUint32), SystemReservedTrxId)
	})

	t.Run("予約値は 0 ではない (未割り当てを示すゼロ値と区別される)", func(t *testing.T) {
		// GIVEN
		// WHEN
		// THEN
		assert.NotEqual(t, TrxId(0), SystemReservedTrxId)
	})

	t.Run("予約値はユーザー採番開始値 1 と異なる", func(t *testing.T) {
		// GIVEN
		// WHEN
		// THEN
		assert.NotEqual(t, TrxId(1), SystemReservedTrxId)
	})
}

package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransactionNewMtr(t *testing.T) {
	t.Run("書き込み mtr が新規生成される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()

		// WHEN
		mtr := trx.NewMtr()

		// THEN
		assert.NotNil(t, mtr)
		assert.Equal(t, 0, mtr.PinnedCount())
		assert.Equal(t, 0, mtr.HeldLatchCount())

		mtr.UnpinAll()
	})

	t.Run("呼ぶたびに異なる mtr インスタンスが返る", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()

		// WHEN
		mtr1 := trx.NewMtr()
		mtr2 := trx.NewMtr()

		// THEN
		assert.NotSame(t, mtr1, mtr2)

		mtr1.UnpinAll()
		mtr2.UnpinAll()
	})
}

func TestTransactionNewReadMtr(t *testing.T) {
	t.Run("読み取り mtr が新規生成される", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()

		// WHEN
		mtr := trx.NewReadMtr()

		// THEN
		assert.NotNil(t, mtr)
		assert.Equal(t, 0, mtr.PinnedCount())
		assert.Equal(t, 0, mtr.HeldLatchCount())

		mtr.UnpinAll()
	})

	t.Run("呼ぶたびに異なる mtr インスタンスが返る", func(t *testing.T) {
		// GIVEN
		tm := setupTrxManager(t)
		trx := tm.Begin()

		// WHEN
		mtr1 := trx.NewReadMtr()
		mtr2 := trx.NewReadMtr()

		// THEN
		assert.NotSame(t, mtr1, mtr2)

		mtr1.UnpinAll()
		mtr2.UnpinAll()
	})
}

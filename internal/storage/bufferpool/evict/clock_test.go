package evict

import (
	"testing"

	"minesql/internal/storage/bufferpool/buftype"

	"github.com/stretchr/testify/assert"
)

func TestClockSweep_Evict(t *testing.T) {
	t.Run("参照されていないページが追い出される", func(t *testing.T) {
		// GIVEN: 3 スロット、すべて未参照
		policy := NewClockSweep(3)

		// WHEN
		victim := policy.Evict()

		// THEN: ポインタ位置 0 が追い出される
		assert.Equal(t, buftype.BufferId(0), victim)
	})

	t.Run("参照されているページはスキップされ、参照ビットがクリアされる", func(t *testing.T) {
		// GIVEN: スロット 0, 1 が参照済み、スロット 2 が未参照
		policy := NewClockSweep(3)
		policy.Access(buftype.BufferId(0))
		policy.Access(buftype.BufferId(1))

		// WHEN
		victim := policy.Evict()

		// THEN: スロット 0, 1 はスキップされ、スロット 2 が追い出される
		assert.Equal(t, buftype.BufferId(2), victim)
	})

	t.Run("すべてのページが参照されている場合、一周してポインタ位置のページが追い出される", func(t *testing.T) {
		// GIVEN: すべてのスロットが参照済み
		policy := NewClockSweep(3)
		policy.Access(buftype.BufferId(0))
		policy.Access(buftype.BufferId(1))
		policy.Access(buftype.BufferId(2))

		// WHEN
		victim := policy.Evict()

		// THEN: 一周して全参照ビットがクリアされ、ポインタ初期位置 (0) が追い出される
		assert.Equal(t, buftype.BufferId(0), victim)
	})

	t.Run("追い出し後にポインタが進む", func(t *testing.T) {
		// GIVEN
		policy := NewClockSweep(3)

		// WHEN: 2 回追い出す
		victim1 := policy.Evict()
		victim2 := policy.Evict()

		// THEN: 0, 1 の順に追い出される
		assert.Equal(t, buftype.BufferId(0), victim1)
		assert.Equal(t, buftype.BufferId(1), victim2)
	})

	t.Run("ポインタが末尾に達すると先頭に戻る", func(t *testing.T) {
		// GIVEN: 2 スロット
		policy := NewClockSweep(2)

		// WHEN: 3 回追い出す
		victim1 := policy.Evict()
		victim2 := policy.Evict()
		victim3 := policy.Evict()

		// THEN: 0, 1, 0 の順に追い出される (ラップアラウンド)
		assert.Equal(t, buftype.BufferId(0), victim1)
		assert.Equal(t, buftype.BufferId(1), victim2)
		assert.Equal(t, buftype.BufferId(0), victim3)
	})
}

func TestClockSweep_Access(t *testing.T) {
	t.Run("アクセスしたページは追い出しでスキップされる", func(t *testing.T) {
		// GIVEN: スロット 0 をアクセス済みにする
		policy := NewClockSweep(3)
		policy.Access(buftype.BufferId(0))

		// WHEN
		victim := policy.Evict()

		// THEN: スロット 0 はスキップされ、スロット 1 が追い出される
		assert.Equal(t, buftype.BufferId(1), victim)
	})
}

func TestClockSweep_Remove(t *testing.T) {
	t.Run("Remove したページが優先的に追い出される", func(t *testing.T) {
		// GIVEN: すべてのスロットを参照済みにした後、スロット 1 の参照を解除
		policy := NewClockSweep(3)
		policy.Access(buftype.BufferId(0))
		policy.Access(buftype.BufferId(1))
		policy.Access(buftype.BufferId(2))
		policy.Remove(buftype.BufferId(1))

		// WHEN
		victim := policy.Evict()

		// THEN: スロット 0 はスキップ (参照クリア)、スロット 1 が追い出される
		assert.Equal(t, buftype.BufferId(1), victim)
	})
}

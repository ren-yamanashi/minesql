package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsCompatible(t *testing.T) {
	t.Run("保持者がいない場合は Shared と互換性がある", func(t *testing.T) {
		// GIVEN
		s := newState()

		// WHEN
		result := s.isCompatible(Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("保持者がいない場合は Exclusive と互換性がある", func(t *testing.T) {
		// GIVEN
		s := newState()

		// WHEN
		result := s.isCompatible(Exclusive)

		// THEN
		assert.True(t, result)
	})

	t.Run("Shared 保持中に Shared は互換性がある", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Shared

		// WHEN
		result := s.isCompatible(Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("複数の Shared 保持中に Shared は互換性がある", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Shared
		s.holders[2] = Shared

		// WHEN
		result := s.isCompatible(Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("Shared 保持中に Exclusive は互換性がない", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Shared

		// WHEN
		result := s.isCompatible(Exclusive)

		// THEN
		assert.False(t, result)
	})

	t.Run("Exclusive 保持中に Shared は互換性がない", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Exclusive

		// WHEN
		result := s.isCompatible(Shared)

		// THEN
		assert.False(t, result)
	})

	t.Run("Exclusive 保持中に Exclusive は互換性がない", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Exclusive

		// WHEN
		result := s.isCompatible(Exclusive)

		// THEN
		assert.False(t, result)
	})

	t.Run("不正なモードは互換性がない", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Shared

		// WHEN
		result := s.isCompatible(Mode(99))

		// THEN
		assert.False(t, result)
	})
}

func TestCanGrant(t *testing.T) {
	t.Run("保持者も待機者もいない場合は Shared を付与できる", func(t *testing.T) {
		// GIVEN
		s := newState()

		// WHEN
		result := s.canGrant(1, Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("保持者も待機者もいない場合は Exclusive を付与できる", func(t *testing.T) {
		// GIVEN
		s := newState()

		// WHEN
		result := s.canGrant(1, Exclusive)

		// THEN
		assert.True(t, result)
	})

	t.Run("他のトランザクションが Shared を保持中に Shared を付与できる", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Shared

		// WHEN
		result := s.canGrant(2, Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("他のトランザクションが Shared を保持中に Exclusive は付与できない", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Shared

		// WHEN
		result := s.canGrant(2, Exclusive)

		// THEN
		assert.False(t, result)
	})

	t.Run("他のトランザクションが Exclusive を保持中に Shared は付与できない", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Exclusive

		// WHEN
		result := s.canGrant(2, Shared)

		// THEN
		assert.False(t, result)
	})

	t.Run("自身が Shared を保持中に同じ Shared を要求すると付与できる", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Shared

		// WHEN
		result := s.canGrant(1, Shared)

		// THEN
		assert.True(t, result)
	})

	t.Run("自身が Exclusive を保持中に同じ Exclusive を要求すると付与できる", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Exclusive

		// WHEN
		result := s.canGrant(1, Exclusive)

		// THEN
		assert.True(t, result)
	})

	t.Run("自身が唯一の Shared 保持者なら Exclusive に昇格できる", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Shared

		// WHEN
		result := s.canGrant(1, Exclusive)

		// THEN
		assert.True(t, result)
	})

	t.Run("自身が Shared 保持者だが他にも保持者がいる場合は Exclusive に昇格できない", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Shared
		s.holders[2] = Shared

		// WHEN
		result := s.canGrant(1, Exclusive)

		// THEN
		assert.False(t, result)
	})

	t.Run("待機キューに待機者がいる場合は新規の Shared を付与できない", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.waitQueue = append(s.waitQueue, &request{trxId: 2, mode: Exclusive})

		// WHEN
		result := s.canGrant(1, Shared)

		// THEN
		assert.False(t, result)
	})

	t.Run("自身が Exclusive を保持中に Shared を要求すると付与できない", func(t *testing.T) {
		// GIVEN
		s := newState()
		s.holders[1] = Exclusive

		// WHEN
		result := s.canGrant(1, Shared)

		// THEN
		assert.False(t, result)
	})
}

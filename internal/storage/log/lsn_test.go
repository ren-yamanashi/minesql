package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLSNGenerator(t *testing.T) {
	t.Run("指定した初期値で生成される", func(t *testing.T) {
		// GIVEN / WHEN
		gen := NewLSNGenerator(100)

		// THEN
		assert.Equal(t, LSN(100), gen.LastGenerated)
	})
}

func TestAllocateLSN(t *testing.T) {
	t.Run("初期値が 0 の場合、1 から採番される", func(t *testing.T) {
		// GIVEN
		gen := NewLSNGenerator(0)

		// WHEN
		lsn1 := gen.AllocateLSN()
		lsn2 := gen.AllocateLSN()
		lsn3 := gen.AllocateLSN()

		// THEN
		assert.Equal(t, LSN(1), lsn1)
		assert.Equal(t, LSN(2), lsn2)
		assert.Equal(t, LSN(3), lsn3)
	})

	t.Run("初期値を指定して開始できる", func(t *testing.T) {
		// GIVEN
		gen := NewLSNGenerator(100)

		// WHEN
		lsn := gen.AllocateLSN()

		// THEN
		assert.Equal(t, LSN(101), lsn)
	})
}

func TestLastGenerated(t *testing.T) {
	t.Run("最後に採番した LSN が記録される", func(t *testing.T) {
		// GIVEN
		gen := NewLSNGenerator(0)
		gen.AllocateLSN()
		gen.AllocateLSN()

		// WHEN
		current := gen.LastGenerated

		// THEN
		assert.Equal(t, LSN(2), current)
	})

	t.Run("Next を呼ぶ前は初期値を返す", func(t *testing.T) {
		// GIVEN
		gen := NewLSNGenerator(50)

		// WHEN
		current := gen.LastGenerated

		// THEN
		assert.Equal(t, LSN(50), current)
	})
}

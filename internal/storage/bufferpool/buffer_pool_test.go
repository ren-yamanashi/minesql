package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBufferPool(t *testing.T) {
	t.Run("正常にバッファプールが生成される", func(t *testing.T) {
		// GIVEN
		poolSize := 10

		// WHEN
		bp := NewBufferPool(poolSize)

		// THEN
		assert.Equal(t, len(bp.BufferPages), poolSize)
		assert.Equal(t, bp.MaxBufferSize, poolSize)
	})
}

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewServer(t *testing.T) {
	t.Run("Server が正しく初期化される", func(t *testing.T) {
		// WHEN
		s := NewServer("localhost", 3307)

		// THEN
		assert.Equal(t, "localhost", s.address)
		assert.Equal(t, 3307, s.port)
	})
}

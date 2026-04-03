package server

import (
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSession(t *testing.T) {
	t.Run("session が初期状態で生成される", func(t *testing.T) {
		// WHEN
		sess := newSession()

		// THEN
		assert.NotNil(t, sess)
		assert.Equal(t, handler.TrxId(0), sess.trxId)
	})
}

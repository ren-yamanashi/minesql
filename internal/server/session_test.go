package server

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/handler"
	"github.com/stretchr/testify/assert"
)

func TestNewSession(t *testing.T) {
	t.Run("session が初期状態で生成される", func(t *testing.T) {
		// WHEN
		sess := newSession("root", serverCapability)

		// THEN
		assert.NotNil(t, sess)
		assert.Equal(t, handler.TrxId(0), sess.trxId)
		assert.Equal(t, "root", sess.username)
		assert.Equal(t, serverCapability, sess.capability)
	})
}

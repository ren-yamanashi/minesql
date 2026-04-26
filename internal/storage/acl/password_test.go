package acl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGeneratePassword(t *testing.T) {
	t.Run("16 文字のパスワードが生成される", func(t *testing.T) {
		// WHEN
		password, err := GeneratePassword()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 16, len(password))
	})

	t.Run("生成されるパスワードが毎回異なる", func(t *testing.T) {
		// WHEN
		p1, err := GeneratePassword()
		assert.NoError(t, err)
		p2, err := GeneratePassword()
		assert.NoError(t, err)

		// THEN
		assert.NotEqual(t, p1, p2)
	})
}

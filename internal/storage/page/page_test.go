package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPage(t *testing.T) {
	t.Run("Header と Body が正しく分割される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)

		// WHEN
		pg := NewPage(data)

		// THEN
		assert.Equal(t, PageHeaderSize, len(pg.Header))
		assert.Equal(t, PageSize-PageHeaderSize, len(pg.Body))
	})
}

func TestPageHeader(t *testing.T) {
	t.Run("Header が data[0:4] を返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
		data[0] = 0xAB

		// WHEN
		pg := NewPage(data)

		// THEN
		assert.Equal(t, byte(0xAB), pg.Header[0])
	})

	t.Run("Header への書き込みが元の data に反映される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
		pg := NewPage(data)

		// WHEN
		pg.Header[0] = 0xFF

		// THEN
		assert.Equal(t, byte(0xFF), data[0])
	})

	t.Run("Header の変更が Body のデータに影響しない", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
		pg := NewPage(data)
		copy(pg.Body[0:5], []byte("hello"))

		// WHEN
		pg.Header[0] = 0xFF
		pg.Header[1] = 0xFF
		pg.Header[2] = 0xFF
		pg.Header[3] = 0xFF

		// THEN
		assert.Equal(t, []byte("hello"), pg.Body[0:5])
	})
}

func TestPageBody(t *testing.T) {
	t.Run("Body が data[4:] を返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
		data[4] = 0xAB

		// WHEN
		pg := NewPage(data)

		// THEN
		assert.Equal(t, byte(0xAB), pg.Body[0])
	})

	t.Run("Body への書き込みが元の data に反映される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, PageSize)
		pg := NewPage(data)

		// WHEN
		pg.Body[0] = 0xFF

		// THEN
		assert.Equal(t, byte(0xFF), data[PageHeaderSize])
	})
}

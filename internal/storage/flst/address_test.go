package flst

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestAddressIsInvalid(t *testing.T) {
	t.Run("InvalidAddress は無効と判定される", func(t *testing.T) {
		// THEN
		assert.True(t, InvalidAddress().IsInvalid())
	})

	t.Run("PageNumber が MaxPageNumber なら Offset に関わらず無効と判定される", func(t *testing.T) {
		// GIVEN
		addr := Address{PageNumber: page.MaxPageNumber, Offset: 42}

		// THEN
		assert.True(t, addr.IsInvalid())
	})

	t.Run("有効な PageNumber は無効と判定されない", func(t *testing.T) {
		// GIVEN
		addr := Address{PageNumber: 0, Offset: 0}

		// THEN
		assert.False(t, addr.IsInvalid())
	})
}

func TestAddressWriteAt(t *testing.T) {
	t.Run("指定位置に BigEndian で 6 バイトのバイト列として書き込める", func(t *testing.T) {
		// GIVEN
		addr := Address{PageNumber: page.PageNumber(0x01020304), Offset: 0x0506}
		data := make([]byte, 16)

		// WHEN
		addr.WriteAt(data, 3)

		// THEN
		assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}, data[3:3+addressSize])
	})
}

func TestReadAddress(t *testing.T) {
	t.Run("WriteAt で書き込んだ Address を読み戻せる", func(t *testing.T) {
		// GIVEN
		addr := Address{PageNumber: page.PageNumber(0x01020304), Offset: 0x0506}
		data := make([]byte, 16)
		addr.WriteAt(data, 3)

		// WHEN
		got := ReadAddress(data, 3)

		// THEN
		assert.Equal(t, addr, got)
	})

	t.Run("無効アドレスもエンコード往復できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, addressSize)
		InvalidAddress().WriteAt(data, 0)

		// WHEN
		got := ReadAddress(data, 0)

		// THEN
		assert.True(t, got.IsInvalid())
	})
}

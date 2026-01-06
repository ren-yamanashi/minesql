package metapage

import (
	"minesql/internal/storage/disk"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMetaPage(t *testing.T) {
	t.Run("MetaPage インスタンスが生成される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)

		// WHEN
		metaPage := NewMetaPage(data)

		// THEN
		assert.NotNil(t, metaPage)
		assert.Equal(t, data, metaPage.data)
	})

	t.Run("ルートページ ID が正しく読み取れる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		metaPage := NewMetaPage(data)
		metaPage.SetRootPageId(42)

		// WHEN
		rootPageId := metaPage.RootPageId()

		// THEN
		assert.Equal(t, disk.PageId(42), rootPageId)
	})

	t.Run("ルートページ ID が正しく設定できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		metaPage := NewMetaPage(data)

		// WHEN
		metaPage.SetRootPageId(99)

		// THEN
		assert.Equal(t, disk.PageId(99), metaPage.RootPageId())
	})
}

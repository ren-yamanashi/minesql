package metapage

import (
	"minesql/internal/storage/disk"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMetaPage(t *testing.T) {
	t.Run("MetaPage インスタンスが生成される", func(t *testing.T) {
		data := make([]byte, 128)
		metaPage := NewMetaPage(data)

		assert.NotNil(t, metaPage)
		assert.Equal(t, data, metaPage.data)
	})

	t.Run("ルートページ ID が正しく読み取れる", func(t *testing.T) {
		data := make([]byte, 128)
		metaPage := NewMetaPage(data)
		metaPage.SetRootPageId(42)

		rootPageId := metaPage.RootPageId()
		assert.Equal(t, disk.PageId(42), rootPageId)
	})

	t.Run("ルートページ ID が正しく設定できる", func(t *testing.T) {
		data := make([]byte, 128)
		metaPage := NewMetaPage(data)
		metaPage.SetRootPageId(99)

		rootPageId := metaPage.RootPageId()
		assert.Equal(t, disk.PageId(99), rootPageId)
	})
}

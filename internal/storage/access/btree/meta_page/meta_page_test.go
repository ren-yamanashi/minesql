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
		expectedPageId := disk.NewPageId(disk.FileId(1), disk.PageNumber(42))
		metaPage.SetRootPageId(expectedPageId)

		// WHEN
		rootPageId := metaPage.RootPageId()

		// THEN
		assert.Equal(t, expectedPageId, rootPageId)
	})

	t.Run("ルートページ ID が正しく設定できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		metaPage := NewMetaPage(data)
		expectedPageId := disk.NewPageId(disk.FileId(2), disk.PageNumber(99))

		// WHEN
		metaPage.SetRootPageId(expectedPageId)

		// THEN
		assert.Equal(t, expectedPageId, metaPage.RootPageId())
	})
}

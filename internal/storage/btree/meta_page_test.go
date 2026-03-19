package btree

import (
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMetaPage(t *testing.T) {
	t.Run("metaPage インスタンスが生成される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)

		// WHEN
		mp := newMetaPage(data)

		// THEN
		assert.NotNil(t, mp)
		assert.Equal(t, data, mp.data)
	})

	t.Run("ルートページ ID が正しく読み取れる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)
		expectedPageId := page.NewPageId(page.FileId(1), page.PageNumber(42))
		mp.setRootPageId(expectedPageId)

		// WHEN
		rootPageId := mp.rootPageId()

		// THEN
		assert.Equal(t, expectedPageId, rootPageId)
	})

	t.Run("ルートページ ID が正しく設定できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)
		expectedPageId := page.NewPageId(page.FileId(2), page.PageNumber(99))

		// WHEN
		mp.setRootPageId(expectedPageId)

		// THEN
		assert.Equal(t, expectedPageId, mp.rootPageId())
	})
}

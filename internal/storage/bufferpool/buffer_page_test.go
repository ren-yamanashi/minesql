package bufferpool

import (
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBufferPage(t *testing.T) {
	t.Run("正常にバッファページが生成される", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))

		// WHEN
		bufferPage := NewBufferPage(pageId)

		// THEN
		assert.Equal(t, bufferPage.PageId, pageId)
		assert.False(t, bufferPage.IsDirty)
		assert.NotNil(t, bufferPage.Page)
	})
}

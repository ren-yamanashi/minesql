package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewBufferPage(t *testing.T) {
	t.Run("指定した PageId で BufferPage を生成できる", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(1, 0)

		// WHEN
		bp, err := NewBufferPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, bp.PageId)
		assert.NotNil(t, bp.Page)
		assert.False(t, bp.IsDirty)
	})

	t.Run("生成した Page のサイズが PageSize と一致する", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(0, 0)

		// WHEN
		bp, err := NewBufferPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.PageSize-page.PageHeaderSize, len(bp.Page.Body))
		assert.Equal(t, page.PageHeaderSize, len(bp.Page.Header))
	})
}

package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewPage(t *testing.T) {
	t.Run("指定した PageId で Page を生成できる", func(t *testing.T) {
		// GIVEN
		pageId := page.NewId(1, 0)

		// WHEN
		bp, err := newPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, bp.pageId)
		assert.NotNil(t, bp.data)
		assert.False(t, bp.isDirty)
	})

	t.Run("生成した Page のサイズが PageSize と一致する", func(t *testing.T) {
		// GIVEN
		pageId := page.NewId(0, 0)

		// WHEN
		bp, err := newPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.Size-page.HeaderSize, len(bp.data.Body))
		assert.Equal(t, page.HeaderSize, len(bp.data.Header))
	})
}

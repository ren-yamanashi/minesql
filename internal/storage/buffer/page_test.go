package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestPageId(t *testing.T) {
	t.Run("設定された PageId を返す", func(t *testing.T) {
		// GIVEN
		pageId := page.NewId(1, 2)
		bp, err := NewPage(pageId)
		assert.NoError(t, err)

		// WHEN
		result := bp.PageId()

		// THEN
		assert.Equal(t, pageId, result)
	})
}

func TestData(t *testing.T) {
	t.Run("Page のデータを返す", func(t *testing.T) {
		// GIVEN
		pageId := page.NewId(0, 0)
		bp, err := NewPage(pageId)
		assert.NoError(t, err)

		// WHEN
		result := bp.Data()

		// THEN
		assert.NotNil(t, result)
		assert.Equal(t, page.Size-page.HeaderSize, len(result.Body()))
		assert.Equal(t, page.HeaderSize, len(result.Header()))
	})
}

func TestNewPage(t *testing.T) {
	t.Run("指定した PageId で Page を生成できる", func(t *testing.T) {
		// GIVEN
		pageId := page.NewId(1, 0)

		// WHEN
		bp, err := NewPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, bp.pageId)
		assert.NotNil(t, bp.data)
		assert.False(t, bp.isDirty)
		assert.Equal(t, 0, bp.pinCount)
	})

	t.Run("生成した Page のサイズが PageSize と一致する", func(t *testing.T) {
		// GIVEN
		pageId := page.NewId(0, 0)

		// WHEN
		bp, err := NewPage(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.Size-page.HeaderSize, len(bp.data.Body()))
		assert.Equal(t, page.HeaderSize, len(bp.data.Header()))
	})
}

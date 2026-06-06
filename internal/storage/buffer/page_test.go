package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestPageId(t *testing.T) {
	t.Run("設定された PageId を返す", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(1, 2)
		bp, err := NewPage(pageId, pool)
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
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := NewPage(pageId, pool)
		assert.NoError(t, err)

		// WHEN
		result := bp.Data()

		// THEN
		assert.NotNil(t, result)
		assert.Equal(t, page.Size-page.HeaderSize, len(result.Body()))
		assert.Equal(t, page.HeaderSize, len(result.Header()))
	})
}

func TestMarkModified(t *testing.T) {
	t.Run("呼び出すたびに modifyCount が 1 増え isDirty が立つ", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		before := bp.modifyCount

		// WHEN
		bp.MarkModified()
		bp.MarkModified()
		bp.MarkModified()

		// THEN
		assert.Equal(t, before+3, bp.modifyCount)
		assert.True(t, bp.isDirty)
	})
}

func TestNewPage(t *testing.T) {
	t.Run("指定した PageId で Page を生成できる", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(1, 0)

		// WHEN
		bp, err := NewPage(pageId, pool)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, bp.pageId)
		assert.NotNil(t, bp.data)
		assert.False(t, bp.isDirty)
		assert.Equal(t, 0, bp.pinCount)
		assert.NotNil(t, bp.latch)
		assert.Equal(t, uint64(0), bp.modifyCount)
	})

	t.Run("生成した Page のサイズが PageSize と一致する", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)

		// WHEN
		bp, err := NewPage(pageId, pool)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.Size-page.HeaderSize, len(bp.data.Body()))
		assert.Equal(t, page.HeaderSize, len(bp.data.Header()))
	})
}

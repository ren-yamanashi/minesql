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

func TestGetWriteData(t *testing.T) {
	t.Run("IsDirty と Referenced が true に設定される", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		data := bufferPage.GetWriteData()

		// THEN
		assert.True(t, bufferPage.IsDirty)
		assert.True(t, bufferPage.Referenced)
		assert.NotNil(t, data)
		assert.Equal(t, page.PAGE_SIZE, len(data))
	})

	t.Run("データを書き込むと Page の内容が更新される", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		data := bufferPage.GetWriteData()
		testData := []byte("test data")
		copy(data[0:len(testData)], testData)

		// THEN
		assert.Equal(t, testData, bufferPage.Page[0:len(testData)])
		assert.True(t, bufferPage.IsDirty)
		assert.True(t, bufferPage.Referenced)
	})

	t.Run("複数回呼び出しても IsDirty と Referenced が true のまま", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		bufferPage.GetWriteData()
		bufferPage.GetWriteData()

		// THEN
		assert.True(t, bufferPage.IsDirty)
		assert.True(t, bufferPage.Referenced)
	})
}

func TestGetReadData(t *testing.T) {
	t.Run("Referenced のみが true に設定される", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		data := bufferPage.GetReadData()

		// THEN
		assert.False(t, bufferPage.IsDirty)
		assert.True(t, bufferPage.Referenced)
		assert.NotNil(t, data)
		assert.Equal(t, page.PAGE_SIZE, len(data))
	})

	t.Run("IsDirty が false のまま維持される", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := NewBufferPage(pageId)
		bufferPage.IsDirty = false
		bufferPage.Referenced = false

		// WHEN
		bufferPage.GetReadData()

		// THEN
		assert.False(t, bufferPage.IsDirty)
		assert.True(t, bufferPage.Referenced)
	})

	t.Run("データを読み取ると Page の内容にアクセスできる", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := NewBufferPage(pageId)
		testData := []byte("test data")
		copy(bufferPage.Page[0:len(testData)], testData)

		// WHEN
		data := bufferPage.GetReadData()

		// THEN
		assert.Equal(t, testData, data[0:len(testData)])
		assert.False(t, bufferPage.IsDirty)
		assert.True(t, bufferPage.Referenced)
	})

	t.Run("複数回呼び出しても Referenced が true で IsDirty が false のまま", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		bufferPage.GetReadData()
		bufferPage.GetReadData()

		// THEN
		assert.False(t, bufferPage.IsDirty)
		assert.True(t, bufferPage.Referenced)
	})
}

func TestGetWriteDataAndGetReadData(t *testing.T) {
	t.Run("GetWriteData の後に GetReadData を呼んでも IsDirty は true のまま", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		bufferPage.GetWriteData()
		bufferPage.GetReadData()

		// THEN
		assert.True(t, bufferPage.IsDirty)
		assert.True(t, bufferPage.Referenced)
	})

	t.Run("GetReadData の後に GetWriteData を呼ぶと IsDirty が true になる", func(t *testing.T) {
		// GIVEN
		pageId := page.NewPageId(page.FileId(0), page.PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		bufferPage.GetReadData()
		bufferPage.GetWriteData()

		// THEN
		assert.True(t, bufferPage.IsDirty)
		assert.True(t, bufferPage.Referenced)
	})
}

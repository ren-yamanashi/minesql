package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBufferPage(t *testing.T) {
	t.Run("正常にバッファページが生成される", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(FileId(0), PageNumber(0))

		// WHEN
		bufferPage := NewBufferPage(pageId)

		// THEN
		assert.Equal(t, bufferPage.PageId, pageId)
		assert.False(t, bufferPage.IsDirty)
		assert.NotNil(t, bufferPage.Page)
	})
}

func TestGetWriteData(t *testing.T) {
	t.Run("IsDirty が true に設定される", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(FileId(0), PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		data := bufferPage.GetWriteData()

		// THEN
		assert.True(t, bufferPage.IsDirty)
		assert.NotNil(t, data)
		assert.Equal(t, PAGE_SIZE, len(data))
	})

	t.Run("データを書き込むと Page の内容が更新される", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(FileId(0), PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		data := bufferPage.GetWriteData()
		testData := []byte("test data")
		copy(data[0:len(testData)], testData)

		// THEN
		assert.Equal(t, testData, bufferPage.Page[0:len(testData)])
		assert.True(t, bufferPage.IsDirty)
	})

	t.Run("複数回呼び出しても IsDirty が true のまま", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(FileId(0), PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		bufferPage.GetWriteData()
		bufferPage.GetWriteData()

		// THEN
		assert.True(t, bufferPage.IsDirty)
	})
}

func TestGetReadData(t *testing.T) {
	t.Run("IsDirty は false のまま維持される", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(FileId(0), PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		data := bufferPage.GetReadData()

		// THEN
		assert.False(t, bufferPage.IsDirty)
		assert.NotNil(t, data)
		assert.Equal(t, PAGE_SIZE, len(data))
	})

	t.Run("データを読み取ると Page の内容にアクセスできる", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(FileId(0), PageNumber(0))
		bufferPage := NewBufferPage(pageId)
		testData := []byte("test data")
		copy(bufferPage.Page[0:len(testData)], testData)

		// WHEN
		data := bufferPage.GetReadData()

		// THEN
		assert.Equal(t, testData, data[0:len(testData)])
		assert.False(t, bufferPage.IsDirty)
	})
}

func TestGetWriteDataAndGetReadData(t *testing.T) {
	t.Run("GetWriteData の後に GetReadData を呼んでも IsDirty は true のまま", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(FileId(0), PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		bufferPage.GetWriteData()
		bufferPage.GetReadData()

		// THEN
		assert.True(t, bufferPage.IsDirty)
	})

	t.Run("GetReadData の後に GetWriteData を呼ぶと IsDirty が true になる", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(FileId(0), PageNumber(0))
		bufferPage := NewBufferPage(pageId)

		// WHEN
		bufferPage.GetReadData()
		bufferPage.GetWriteData()

		// THEN
		assert.True(t, bufferPage.IsDirty)
	})
}

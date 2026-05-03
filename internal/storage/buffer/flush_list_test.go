package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewFlushList(t *testing.T) {
	t.Run("空のフラッシュリストが作成される", func(t *testing.T) {
		// GIVEN / WHEN
		fl := NewFlushList()

		// THEN
		assert.Equal(t, uint32(0), fl.numOfPage)
		assert.Nil(t, fl.head)
		assert.Nil(t, fl.tail)
	})
}

func TestFlushListAdd(t *testing.T) {
	t.Run("ページを追加するとリストに反映される", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		pageId := page.NewPageId(0, 1)

		// WHEN
		fl.Add(pageId)

		// THEN
		assert.Equal(t, uint32(1), fl.numOfPage)
		assert.Equal(t, pageId, fl.head.pageId)
		assert.Equal(t, pageId, fl.tail.pageId)
	})

	t.Run("複数ページを追加すると追加順に並ぶ", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		id1 := page.NewPageId(0, 1)
		id2 := page.NewPageId(0, 2)
		id3 := page.NewPageId(0, 3)

		// WHEN
		fl.Add(id1)
		fl.Add(id2)
		fl.Add(id3)

		// THEN
		assert.Equal(t, uint32(3), fl.numOfPage)
		assert.Equal(t, id1, fl.head.pageId)
		assert.Equal(t, id3, fl.tail.pageId)
	})

	t.Run("同じ PageId を重複追加しても無視される", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		pageId := page.NewPageId(0, 1)
		fl.Add(pageId)

		// WHEN
		fl.Add(pageId)

		// THEN
		assert.Equal(t, uint32(1), fl.numOfPage)
	})
}

func TestFlushListDelete(t *testing.T) {
	t.Run("先頭のページを削除できる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		id1 := page.NewPageId(0, 1)
		id2 := page.NewPageId(0, 2)
		fl.Add(id1)
		fl.Add(id2)

		// WHEN
		fl.Delete(id1)

		// THEN
		assert.Equal(t, uint32(1), fl.numOfPage)
		assert.Equal(t, id2, fl.head.pageId)
	})

	t.Run("末尾のページを削除できる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		id1 := page.NewPageId(0, 1)
		id2 := page.NewPageId(0, 2)
		fl.Add(id1)
		fl.Add(id2)

		// WHEN
		fl.Delete(id2)

		// THEN
		assert.Equal(t, uint32(1), fl.numOfPage)
		assert.Equal(t, id1, fl.tail.pageId)
	})

	t.Run("中間のページを削除できる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		id1 := page.NewPageId(0, 1)
		id2 := page.NewPageId(0, 2)
		id3 := page.NewPageId(0, 3)
		fl.Add(id1)
		fl.Add(id2)
		fl.Add(id3)

		// WHEN
		fl.Delete(id2)

		// THEN
		assert.Equal(t, uint32(2), fl.numOfPage)
		assert.Equal(t, id1, fl.head.pageId)
		assert.Equal(t, id3, fl.tail.pageId)
	})

	t.Run("唯一のページを削除するとリストが空になる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		pageId := page.NewPageId(0, 1)
		fl.Add(pageId)

		// WHEN
		fl.Delete(pageId)

		// THEN
		assert.Equal(t, uint32(0), fl.numOfPage)
		assert.Nil(t, fl.head)
		assert.Nil(t, fl.tail)
	})

	t.Run("存在しない PageId を削除しても何も起きない", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(0, 1))

		// WHEN
		fl.Delete(page.NewPageId(0, 99))

		// THEN
		assert.Equal(t, uint32(1), fl.numOfPage)
	})
}

func TestFlushListClear(t *testing.T) {
	t.Run("リストをクリアすると空になる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(0, 1))
		fl.Add(page.NewPageId(0, 2))

		// WHEN
		fl.Clear()

		// THEN
		assert.Equal(t, uint32(0), fl.numOfPage)
		assert.Nil(t, fl.head)
		assert.Nil(t, fl.tail)
	})

	t.Run("空のリストをクリアしてもエラーにならない", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()

		// WHEN
		fl.Clear()

		// THEN
		assert.Equal(t, uint32(0), fl.numOfPage)
	})
}

func TestFlushListOldestPageIds(t *testing.T) {
	t.Run("先頭から n 件の PageId を返す", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		id1 := page.NewPageId(0, 1)
		id2 := page.NewPageId(0, 2)
		id3 := page.NewPageId(0, 3)
		fl.Add(id1)
		fl.Add(id2)
		fl.Add(id3)

		// WHEN
		result := fl.OldestPageIds(2)

		// THEN
		assert.Equal(t, []page.PageId{id1, id2}, result)
	})

	t.Run("リストのページ数より多い n を指定すると全件返す", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		id1 := page.NewPageId(0, 1)
		id2 := page.NewPageId(0, 2)
		fl.Add(id1)
		fl.Add(id2)

		// WHEN
		result := fl.OldestPageIds(10)

		// THEN
		assert.Equal(t, []page.PageId{id1, id2}, result)
	})

	t.Run("空のリストでは空のスライスを返す", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()

		// WHEN
		result := fl.OldestPageIds(5)

		// THEN
		assert.Equal(t, []page.PageId{}, result)
	})
}

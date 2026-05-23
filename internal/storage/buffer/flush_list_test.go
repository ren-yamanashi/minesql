package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewFlushList(t *testing.T) {
	t.Run("空のフラッシュリストが作成される", func(t *testing.T) {
		// GIVEN / WHEN
		fl := newFlushList()

		// THEN
		assert.Equal(t, 0, fl.NumOfPage)
		assert.Nil(t, fl.Head)
		assert.Nil(t, fl.Tail)
	})
}

func TestFlushListAdd(t *testing.T) {
	t.Run("ページを追加するとリストに反映される", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		pageId := page.NewId(0, 1)

		// WHEN
		fl.add(pageId)

		// THEN
		assert.Equal(t, 1, fl.NumOfPage)
		assert.Equal(t, pageId, fl.Head.pageId)
		assert.Equal(t, pageId, fl.Tail.pageId)
	})

	t.Run("複数ページを追加すると追加順に並ぶ", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		id1 := page.NewId(0, 1)
		id2 := page.NewId(0, 2)
		id3 := page.NewId(0, 3)

		// WHEN
		fl.add(id1)
		fl.add(id2)
		fl.add(id3)

		// THEN
		assert.Equal(t, 3, fl.NumOfPage)
		assert.Equal(t, id1, fl.Head.pageId)
		assert.Equal(t, id3, fl.Tail.pageId)
	})

	t.Run("同じ PageId を重複追加しても無視される", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		pageId := page.NewId(0, 1)
		fl.add(pageId)

		// WHEN
		fl.add(pageId)

		// THEN
		assert.Equal(t, 1, fl.NumOfPage)
	})
}

func TestFlushListDelete(t *testing.T) {
	t.Run("先頭のページを削除できる", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		id1 := page.NewId(0, 1)
		id2 := page.NewId(0, 2)
		fl.add(id1)
		fl.add(id2)

		// WHEN
		fl.delete(id1)

		// THEN
		assert.Equal(t, 1, fl.NumOfPage)
		assert.Equal(t, id2, fl.Head.pageId)
	})

	t.Run("末尾のページを削除できる", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		id1 := page.NewId(0, 1)
		id2 := page.NewId(0, 2)
		fl.add(id1)
		fl.add(id2)

		// WHEN
		fl.delete(id2)

		// THEN
		assert.Equal(t, 1, fl.NumOfPage)
		assert.Equal(t, id1, fl.Tail.pageId)
	})

	t.Run("中間のページを削除できる", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		id1 := page.NewId(0, 1)
		id2 := page.NewId(0, 2)
		id3 := page.NewId(0, 3)
		fl.add(id1)
		fl.add(id2)
		fl.add(id3)

		// WHEN
		fl.delete(id2)

		// THEN
		assert.Equal(t, 2, fl.NumOfPage)
		assert.Equal(t, id1, fl.Head.pageId)
		assert.Equal(t, id3, fl.Tail.pageId)
	})

	t.Run("唯一のページを削除するとリストが空になる", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		pageId := page.NewId(0, 1)
		fl.add(pageId)

		// WHEN
		fl.delete(pageId)

		// THEN
		assert.Equal(t, 0, fl.NumOfPage)
		assert.Nil(t, fl.Head)
		assert.Nil(t, fl.Tail)
	})

	t.Run("存在しない PageId を削除しても何も起きない", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		fl.add(page.NewId(0, 1))

		// WHEN
		fl.delete(page.NewId(0, 99))

		// THEN
		assert.Equal(t, 1, fl.NumOfPage)
	})
}

func TestFlushListClear(t *testing.T) {
	t.Run("リストをクリアすると空になる", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		fl.add(page.NewId(0, 1))
		fl.add(page.NewId(0, 2))

		// WHEN
		fl.clear()

		// THEN
		assert.Equal(t, 0, fl.NumOfPage)
		assert.Nil(t, fl.Head)
		assert.Nil(t, fl.Tail)
	})

	t.Run("空のリストをクリアしてもエラーにならない", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()

		// WHEN
		fl.clear()

		// THEN
		assert.Equal(t, 0, fl.NumOfPage)
	})
}

func TestFlushListOldestPageIds(t *testing.T) {
	t.Run("先頭から n 件の PageId を返す", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		id1 := page.NewId(0, 1)
		id2 := page.NewId(0, 2)
		id3 := page.NewId(0, 3)
		fl.add(id1)
		fl.add(id2)
		fl.add(id3)

		// WHEN
		result := fl.oldestPageIds(2)

		// THEN
		assert.Equal(t, []page.Id{id1, id2}, result)
	})

	t.Run("リストのページ数より多い n を指定すると全件返す", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()
		id1 := page.NewId(0, 1)
		id2 := page.NewId(0, 2)
		fl.add(id1)
		fl.add(id2)

		// WHEN
		result := fl.oldestPageIds(10)

		// THEN
		assert.Equal(t, []page.Id{id1, id2}, result)
	})

	t.Run("空のリストでは空のスライスを返す", func(t *testing.T) {
		// GIVEN
		fl := newFlushList()

		// WHEN
		result := fl.oldestPageIds(5)

		// THEN
		assert.Equal(t, []page.Id{}, result)
	})
}

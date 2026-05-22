package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewPageTable(t *testing.T) {
	t.Run("空のページテーブルが作成される", func(t *testing.T) {
		// GIVEN / WHEN
		pt := newPageTable()

		// THEN
		_, exists := pt.getBufferId(page.NewPageId(0, 0))
		assert.False(t, exists)
	})
}

func TestPageTableGetBufferId(t *testing.T) {
	t.Run("存在する PageId の BufferId を取得できる", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pageId := page.NewPageId(0, 1)
		pt.add(pageId, BufferId(5))

		// WHEN
		bufId, exists := pt.getBufferId(pageId)

		// THEN
		assert.True(t, exists)
		assert.Equal(t, BufferId(5), bufId)
	})

	t.Run("存在しない PageId の場合 false を返す", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()

		// WHEN
		_, exists := pt.getBufferId(page.NewPageId(0, 99))

		// THEN
		assert.False(t, exists)
	})
}

func TestPageTableAdd(t *testing.T) {
	t.Run("エントリを追加すると取得できる", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pageId := page.NewPageId(1, 0)

		// WHEN
		pt.add(pageId, BufferId(3))

		// THEN
		bufId, exists := pt.getBufferId(pageId)
		assert.True(t, exists)
		assert.Equal(t, BufferId(3), bufId)
	})

	t.Run("同じ PageId で追加すると BufferId が上書きされる", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pageId := page.NewPageId(0, 1)
		pt.add(pageId, BufferId(1))

		// WHEN
		pt.add(pageId, BufferId(2))

		// THEN
		bufId, _ := pt.getBufferId(pageId)
		assert.Equal(t, BufferId(2), bufId)
	})
}

func TestPageTableUpdate(t *testing.T) {
	t.Run("追い出しページを削除し新しいページを追加する", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		evictId := page.NewPageId(0, 1)
		newId := page.NewPageId(0, 2)
		pt.add(evictId, BufferId(0))

		// WHEN
		pt.update(evictId, newId, BufferId(0))

		// THEN
		_, evictExists := pt.getBufferId(evictId)
		assert.False(t, evictExists)
		bufId, newExists := pt.getBufferId(newId)
		assert.True(t, newExists)
		assert.Equal(t, BufferId(0), bufId)
	})

	t.Run("追い出しページの BufferId が一致しない場合は削除されない", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		evictId := page.NewPageId(0, 1)
		newId := page.NewPageId(0, 2)
		pt.add(evictId, BufferId(0))

		// WHEN
		pt.update(evictId, newId, BufferId(99)) // BufferId が一致しない

		// THEN
		_, evictExists := pt.getBufferId(evictId)
		assert.True(t, evictExists) // 削除されない
		bufId, newExists := pt.getBufferId(newId)
		assert.True(t, newExists)
		assert.Equal(t, BufferId(99), bufId)
	})

	t.Run("追い出しページが存在しない場合は新しいページだけ追加される", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		evictId := page.NewPageId(0, 1)
		newId := page.NewPageId(0, 2)

		// WHEN
		pt.update(evictId, newId, BufferId(0))

		// THEN
		_, evictExists := pt.getBufferId(evictId)
		assert.False(t, evictExists)
		bufId, newExists := pt.getBufferId(newId)
		assert.True(t, newExists)
		assert.Equal(t, BufferId(0), bufId)
	})
}

func TestPageTableDelete(t *testing.T) {
	t.Run("存在する PageId を削除できる", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pageId := page.NewPageId(0, 1)
		pt.add(pageId, BufferId(0))

		// WHEN
		pt.delete(pageId)

		// THEN
		_, exists := pt.getBufferId(pageId)
		assert.False(t, exists)
	})

	t.Run("存在しない PageId を削除しても何も起きない", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pt.add(page.NewPageId(0, 1), BufferId(0))

		// WHEN
		pt.delete(page.NewPageId(0, 99))

		// THEN
		_, exists := pt.getBufferId(page.NewPageId(0, 1))
		assert.True(t, exists)
	})
}

func TestPageTableForEach(t *testing.T) {
	t.Run("全エントリに対してコールバックが実行される", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		id1 := page.NewPageId(0, 1)
		id2 := page.NewPageId(0, 2)
		pt.add(id1, BufferId(10))
		pt.add(id2, BufferId(20))

		// WHEN
		visited := map[page.PageId]BufferId{}
		pt.forEach(func(pageId page.PageId, bufferId BufferId) {
			visited[pageId] = bufferId
		})

		// THEN
		assert.Equal(t, 2, len(visited))
		assert.Equal(t, BufferId(10), visited[id1])
		assert.Equal(t, BufferId(20), visited[id2])
	})

	t.Run("空のテーブルではコールバックが呼ばれない", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()

		// WHEN
		count := 0
		pt.forEach(func(pageId page.PageId, bufferId BufferId) {
			count++
		})

		// THEN
		assert.Equal(t, 0, count)
	})
}

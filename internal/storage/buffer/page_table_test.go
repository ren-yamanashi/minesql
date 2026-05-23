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
		_, exists := pt.bufferId(page.NewId(0, 0))
		assert.False(t, exists)
	})
}

func TestPageTableGetBufferId(t *testing.T) {
	t.Run("存在する PageId の BufferId を取得できる", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pageId := page.NewId(0, 1)
		pt.add(pageId, id(5))

		// WHEN
		bufId, exists := pt.bufferId(pageId)

		// THEN
		assert.True(t, exists)
		assert.Equal(t, id(5), bufId)
	})

	t.Run("存在しない PageId の場合 false を返す", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()

		// WHEN
		_, exists := pt.bufferId(page.NewId(0, 99))

		// THEN
		assert.False(t, exists)
	})
}

func TestPageTableAdd(t *testing.T) {
	t.Run("エントリを追加すると取得できる", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pageId := page.NewId(1, 0)

		// WHEN
		pt.add(pageId, id(3))

		// THEN
		bufId, exists := pt.bufferId(pageId)
		assert.True(t, exists)
		assert.Equal(t, id(3), bufId)
	})

	t.Run("同じ PageId で追加すると BufferId が上書きされる", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pageId := page.NewId(0, 1)
		pt.add(pageId, id(1))

		// WHEN
		pt.add(pageId, id(2))

		// THEN
		bufId, _ := pt.bufferId(pageId)
		assert.Equal(t, id(2), bufId)
	})
}

func TestPageTableUpdate(t *testing.T) {
	t.Run("追い出しページを削除し新しいページを追加する", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		evictId := page.NewId(0, 1)
		newId := page.NewId(0, 2)
		pt.add(evictId, id(0))

		// WHEN
		pt.update(evictId, newId, id(0))

		// THEN
		_, evictExists := pt.bufferId(evictId)
		assert.False(t, evictExists)
		bufId, newExists := pt.bufferId(newId)
		assert.True(t, newExists)
		assert.Equal(t, id(0), bufId)
	})

	t.Run("追い出しページの BufferId が一致しない場合は削除されない", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		evictId := page.NewId(0, 1)
		newId := page.NewId(0, 2)
		pt.add(evictId, id(0))

		// WHEN
		pt.update(evictId, newId, id(99)) // BufferId が一致しない

		// THEN
		_, evictExists := pt.bufferId(evictId)
		assert.True(t, evictExists) // 削除されない
		bufId, newExists := pt.bufferId(newId)
		assert.True(t, newExists)
		assert.Equal(t, id(99), bufId)
	})

	t.Run("追い出しページが存在しない場合は新しいページだけ追加される", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		evictId := page.NewId(0, 1)
		newId := page.NewId(0, 2)

		// WHEN
		pt.update(evictId, newId, id(0))

		// THEN
		_, evictExists := pt.bufferId(evictId)
		assert.False(t, evictExists)
		bufId, newExists := pt.bufferId(newId)
		assert.True(t, newExists)
		assert.Equal(t, id(0), bufId)
	})
}

func TestPageTableDelete(t *testing.T) {
	t.Run("存在する PageId を削除できる", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pageId := page.NewId(0, 1)
		pt.add(pageId, id(0))

		// WHEN
		pt.delete(pageId)

		// THEN
		_, exists := pt.bufferId(pageId)
		assert.False(t, exists)
	})

	t.Run("存在しない PageId を削除しても何も起きない", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		pt.add(page.NewId(0, 1), id(0))

		// WHEN
		pt.delete(page.NewId(0, 99))

		// THEN
		_, exists := pt.bufferId(page.NewId(0, 1))
		assert.True(t, exists)
	})
}

func TestPageTableForEach(t *testing.T) {
	t.Run("全エントリに対してコールバックが実行される", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()
		id1 := page.NewId(0, 1)
		id2 := page.NewId(0, 2)
		pt.add(id1, id(10))
		pt.add(id2, id(20))

		// WHEN
		visited := map[page.Id]id{}
		pt.forEach(func(pageId page.Id, bufferId id) {
			visited[pageId] = bufferId
		})

		// THEN
		assert.Equal(t, 2, len(visited))
		assert.Equal(t, id(10), visited[id1])
		assert.Equal(t, id(20), visited[id2])
	})

	t.Run("空のテーブルではコールバックが呼ばれない", func(t *testing.T) {
		// GIVEN
		pt := newPageTable()

		// WHEN
		count := 0
		pt.forEach(func(pageId page.Id, bufferId id) {
			count++
		})

		// THEN
		assert.Equal(t, 0, count)
	})
}

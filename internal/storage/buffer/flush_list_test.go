package buffer

import (
	"encoding/binary"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestFlushListAdd(t *testing.T) {
	t.Run("ページが末尾に追加される", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()

		// WHEN
		fl.Add(page.NewPageId(1, 0))
		fl.Add(page.NewPageId(1, 1))

		// THEN
		assert.Equal(t, 2, fl.Size)
		assert.True(t, fl.Contains(page.NewPageId(1, 0)))
		assert.True(t, fl.Contains(page.NewPageId(1, 1)))
	})

	t.Run("同じページを 2 回追加しても重複しない", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(1, 0))

		// WHEN
		fl.Add(page.NewPageId(1, 0))

		// THEN
		assert.Equal(t, 1, fl.Size)
	})
}

func TestFlushListRemove(t *testing.T) {
	t.Run("先頭のページを削除できる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(1, 0))
		fl.Add(page.NewPageId(1, 1))

		// WHEN
		fl.Remove(page.NewPageId(1, 0))

		// THEN
		assert.Equal(t, 1, fl.Size)
		assert.False(t, fl.Contains(page.NewPageId(1, 0)))
		assert.True(t, fl.Contains(page.NewPageId(1, 1)))
	})

	t.Run("末尾のページを削除できる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(1, 0))
		fl.Add(page.NewPageId(1, 1))

		// WHEN
		fl.Remove(page.NewPageId(1, 1))

		// THEN
		assert.Equal(t, 1, fl.Size)
		assert.True(t, fl.Contains(page.NewPageId(1, 0)))
	})

	t.Run("中間のページを削除できる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(1, 0))
		fl.Add(page.NewPageId(1, 1))
		fl.Add(page.NewPageId(1, 2))

		// WHEN
		fl.Remove(page.NewPageId(1, 1))

		// THEN
		assert.Equal(t, 2, fl.Size)
		ids := fl.OldestPageIds(2)
		assert.Equal(t, page.NewPageId(1, 0), ids[0])
		assert.Equal(t, page.NewPageId(1, 2), ids[1])
	})

	t.Run("存在しないページを削除しても何も起きない", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(1, 0))

		// WHEN
		fl.Remove(page.NewPageId(1, 99))

		// THEN
		assert.Equal(t, 1, fl.Size)
	})

	t.Run("唯一のページを削除するとリストが空になる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(1, 0))

		// WHEN
		fl.Remove(page.NewPageId(1, 0))

		// THEN
		assert.Equal(t, 0, fl.Size)
		assert.Nil(t, fl.head)
		assert.Nil(t, fl.tail)
	})
}

func TestFlushListOldestPageIds(t *testing.T) {
	t.Run("追加順に返される", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(1, 2))
		fl.Add(page.NewPageId(1, 0))
		fl.Add(page.NewPageId(1, 1))

		// WHEN
		ids := fl.OldestPageIds(3)

		// THEN: 追加順 (ダーティーになった順)
		assert.Equal(t, page.NewPageId(1, 2), ids[0])
		assert.Equal(t, page.NewPageId(1, 0), ids[1])
		assert.Equal(t, page.NewPageId(1, 1), ids[2])
	})

	t.Run("n がリストサイズより大きい場合は全件返す", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(1, 0))

		// WHEN
		ids := fl.OldestPageIds(10)

		// THEN
		assert.Equal(t, 1, len(ids))
	})

	t.Run("空リストの場合は空スライスを返す", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()

		// WHEN
		ids := fl.OldestPageIds(5)

		// THEN
		assert.Empty(t, ids)
	})
}

func TestFlushListClear(t *testing.T) {
	t.Run("クリア後はリストが空になる", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		fl.Add(page.NewPageId(1, 0))
		fl.Add(page.NewPageId(1, 1))

		// WHEN
		fl.Clear()

		// THEN
		assert.Equal(t, 0, fl.Size)
		assert.Nil(t, fl.head)
		assert.Nil(t, fl.tail)
		assert.False(t, fl.Contains(page.NewPageId(1, 0)))
	})
}

func TestFlushListMinPageLSN(t *testing.T) {
	// ヘルパー: BufferPage を作成し Page LSN を設定する
	makeBufferPage := func(pageId page.PageId, lsn uint32) BufferPage {
		bp := *NewBufferPage(pageId)
		binary.BigEndian.PutUint32(page.NewPage(bp.Page).Header, lsn)
		return bp
	}

	t.Run("最小の Page LSN を返す", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		pageId1 := page.NewPageId(1, 0)
		pageId2 := page.NewPageId(1, 1)
		pageId3 := page.NewPageId(1, 2)
		fl.Add(pageId1)
		fl.Add(pageId2)
		fl.Add(pageId3)

		bufferPages := []BufferPage{
			makeBufferPage(pageId1, 10),
			makeBufferPage(pageId2, 5),
			makeBufferPage(pageId3, 15),
		}
		pageTable := PageTable{
			pageId1: 0,
			pageId2: 1,
			pageId3: 2,
		}

		// WHEN
		minLSN := fl.MinPageLSN(bufferPages, pageTable)

		// THEN
		assert.Equal(t, uint32(5), minLSN)
	})

	t.Run("フラッシュリストが空の場合は 0 を返す", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()

		// WHEN
		minLSN := fl.MinPageLSN(nil, nil)

		// THEN
		assert.Equal(t, uint32(0), minLSN)
	})

	t.Run("ページが 1 つだけの場合はその Page LSN を返す", func(t *testing.T) {
		// GIVEN
		fl := NewFlushList()
		pageId := page.NewPageId(1, 0)
		fl.Add(pageId)

		bufferPages := []BufferPage{makeBufferPage(pageId, 42)}
		pageTable := PageTable{pageId: 0}

		// WHEN
		minLSN := fl.MinPageLSN(bufferPages, pageTable)

		// THEN
		assert.Equal(t, uint32(42), minLSN)
	})
}

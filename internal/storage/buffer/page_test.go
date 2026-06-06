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

func TestOverwritePage(t *testing.T) {
	t.Run("src でページ全体を上書きし isDirty/modifyCount を更新する", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		before := bp.modifyCount
		src := make([]byte, page.Size)
		for i := range src {
			src[i] = byte(i % 256)
		}

		// WHEN
		bp.OverwritePage(src)

		// THEN
		assert.Equal(t, src, bp.data.Bytes())
		assert.True(t, bp.isDirty)
		assert.Equal(t, before+1, bp.modifyCount)
	})

	t.Run("src の長さが page.Size と一致しないと panic する", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		short := make([]byte, page.Size-1)

		// WHEN / THEN
		assert.Panics(t, func() {
			bp.OverwritePage(short)
		})
	})
}

func TestWriteHeaderAt(t *testing.T) {
	t.Run("ヘッダー先頭から src を書き込み isDirty/modifyCount を更新する", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		before := bp.modifyCount
		src := []byte{0xDE, 0xAD, 0xBE, 0xEF}

		// WHEN
		bp.WriteHeaderAt(0, src)

		// THEN
		assert.Equal(t, src, bp.data.Header())
		assert.True(t, bp.isDirty)
		assert.Equal(t, before+1, bp.modifyCount)
	})

	t.Run("オフセット指定で部分書き込みできる", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		src := []byte{0xAB, 0xCD}

		// WHEN
		bp.WriteHeaderAt(1, src)

		// THEN
		assert.Equal(t, byte(0x00), bp.data.Header()[0])
		assert.Equal(t, byte(0xAB), bp.data.Header()[1])
		assert.Equal(t, byte(0xCD), bp.data.Header()[2])
	})

	t.Run("範囲外書き込みで panic する", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		oversized := make([]byte, page.HeaderSize+1)

		// WHEN
		// THEN
		assert.Panics(t, func() {
			bp.WriteHeaderAt(0, oversized)
		})
	})
}

func TestWriteBodyAt(t *testing.T) {
	t.Run("ボディに src を書き込み isDirty/modifyCount を更新する", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		before := bp.modifyCount
		src := []byte{0x01, 0x02, 0x03, 0x04}

		// WHEN
		bp.WriteBodyAt(0, src)

		// THEN
		assert.Equal(t, src, bp.data.Body()[:4])
		assert.True(t, bp.isDirty)
		assert.Equal(t, before+1, bp.modifyCount)
	})

	t.Run("オフセット指定で部分書き込みできる", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		src := []byte{0xFF, 0xEE}

		// WHEN
		bp.WriteBodyAt(10, src)

		// THEN
		assert.Equal(t, byte(0xFF), bp.data.Body()[10])
		assert.Equal(t, byte(0xEE), bp.data.Body()[11])
		assert.Equal(t, byte(0x00), bp.data.Body()[9])
		assert.Equal(t, byte(0x00), bp.data.Body()[12])
	})

	t.Run("範囲外書き込みで panic する", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		bodySize := page.Size - page.HeaderSize
		oversized := make([]byte, bodySize+1)

		// WHEN
		// THEN
		assert.Panics(t, func() {
			bp.WriteBodyAt(0, oversized)
		})
	})

	t.Run("書き込んだ内容が Pool 経由で再フェッチしても反映されている", func(t *testing.T) {
		// GIVEN
		pool := NewPool(page.Size, nil)
		pageId := page.NewId(0, 0)
		bp, err := pool.AddPage(pageId)
		assert.NoError(t, err)
		bp.WriteBodyAt(0, []byte{0xAA})

		// WHEN
		fetched, err := pool.Page(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAA), fetched.data.Body()[0])
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

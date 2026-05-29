package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewMtr(t *testing.T) {
	t.Run("生成直後は記録している Pin が無い", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)

		// WHEN
		mtr := NewMtr(bp)

		// THEN
		assert.Equal(t, 0, mtr.PinnedCount())
	})
}

func TestMtrPageForRead(t *testing.T) {
	t.Run("ページを取得すると Pin が増えスコープに記録される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)

		// WHEN
		bufPage, err := mtr.PageForRead(pageId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pageId, bufPage.PageId())
		assert.Equal(t, 1, pinCountOf(bp, pageId))
		assert.Equal(t, 1, mtr.PinnedCount())
	})
}

func TestMtrPageForWrite(t *testing.T) {
	t.Run("ページを取得すると Pin が増えスコープに記録される", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)

		// WHEN
		bufPage, err := mtr.PageForWrite(pageId)

		// THEN
		assert.NoError(t, err)
		assert.True(t, bufPage.isDirty)
		assert.Equal(t, 1, pinCountOf(bp, pageId))
		assert.Equal(t, 1, mtr.PinnedCount())
	})
}

func TestMtrUnpin(t *testing.T) {
	t.Run("Pin を解放し記録から除外する", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId)
		assert.NoError(t, err)

		// WHEN
		mtr.Unpin(pageId)

		// THEN
		assert.Equal(t, 0, pinCountOf(bp, pageId))
		assert.Equal(t, 0, mtr.PinnedCount())
	})
}

func TestMtrDetach(t *testing.T) {
	t.Run("記録から除外するが Pin は解放しない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId := page.NewId(0, 0)
		_, err := bp.AddPage(pageId)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId)
		assert.NoError(t, err)

		// WHEN
		mtr.Detach(pageId)

		// THEN
		assert.Equal(t, 1, pinCountOf(bp, pageId))
		assert.Equal(t, 0, mtr.PinnedCount())
	})
}

func TestMtrUnpinAll(t *testing.T) {
	t.Run("記録した全ての Pin を解放する", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId1 := page.NewId(0, 0)
		pageId2 := page.NewId(0, 1)
		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId1)
		assert.NoError(t, err)
		_, err = mtr.PageForWrite(pageId2)
		assert.NoError(t, err)

		// WHEN
		mtr.UnpinAll()

		// THEN
		assert.Equal(t, 0, pinCountOf(bp, pageId1))
		assert.Equal(t, 0, pinCountOf(bp, pageId2))
		assert.Equal(t, 0, mtr.PinnedCount())
	})

	t.Run("Detach したページは解放対象に含まれない", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId1 := page.NewId(0, 0)
		pageId2 := page.NewId(0, 1)
		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId1)
		assert.NoError(t, err)
		_, err = mtr.PageForRead(pageId2)
		assert.NoError(t, err)
		mtr.Detach(pageId2)

		// WHEN
		mtr.UnpinAll()

		// THEN
		assert.Equal(t, 0, pinCountOf(bp, pageId1))
		assert.Equal(t, 1, pinCountOf(bp, pageId2))
	})
}

func TestMtrPinnedCount(t *testing.T) {
	t.Run("複数ページの取得で件数が増える", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId1 := page.NewId(0, 0)
		pageId2 := page.NewId(0, 1)
		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		mtr := NewMtr(bp)

		// WHEN
		_, err = mtr.PageForRead(pageId1)
		assert.NoError(t, err)
		_, err = mtr.PageForWrite(pageId2)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 2, mtr.PinnedCount())
	})

	t.Run("解放すると件数が減る", func(t *testing.T) {
		// GIVEN
		bp := NewPool(page.Size*3, nil)
		pageId1 := page.NewId(0, 0)
		pageId2 := page.NewId(0, 1)
		_, err := bp.AddPage(pageId1)
		assert.NoError(t, err)
		_, err = bp.AddPage(pageId2)
		assert.NoError(t, err)
		mtr := NewMtr(bp)
		_, err = mtr.PageForRead(pageId1)
		assert.NoError(t, err)
		_, err = mtr.PageForRead(pageId2)
		assert.NoError(t, err)

		// WHEN
		mtr.Unpin(pageId1)

		// THEN
		assert.Equal(t, 1, mtr.PinnedCount())
	})
}

// pinCountOf は指定ページの現在の pinCount を返す
func pinCountOf(bp *Pool, pageId page.Id) int {
	bufId, ok := bp.pageTable.bufferId(pageId)
	if !ok {
		return -1
	}
	return bp.pages[bufId].pinCount
}

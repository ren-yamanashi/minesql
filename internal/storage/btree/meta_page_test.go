package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestMetaPageRootPageId(t *testing.T) {
	t.Run("設定したルートページ ID を読み取れる", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage(t)
		expected := page.NewId(1, 10)
		mp.setRootPageId(expected)

		// WHEN
		result := mp.rootPageId()

		// THEN
		assert.Equal(t, expected, result)
	})
}

func TestMetaPageLeafPageCount(t *testing.T) {
	t.Run("設定したリーフページ数を読み取れる", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage(t)
		mp.setLeafPageCount(42)

		// WHEN
		result := mp.leafPageCount()

		// THEN
		assert.Equal(t, uint64(42), result)
	})
}

func TestMetaPageHeight(t *testing.T) {
	t.Run("設定した高さを読み取れる", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage(t)
		mp.setHeight(3)

		// WHEN
		result := mp.height()

		// THEN
		assert.Equal(t, uint64(3), result)
	})
}

func TestMetaPageSetRootPageId(t *testing.T) {
	t.Run("ルートページ ID を上書きできる", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage(t)
		mp.setRootPageId(page.NewId(1, 10))

		// WHEN
		mp.setRootPageId(page.NewId(2, 20))

		// THEN
		assert.Equal(t, page.NewId(2, 20), mp.rootPageId())
	})

	t.Run("呼び出すと bufPage の modifyCount が進む", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage(t)
		before := mp.bufPage.ModifyCount()

		// WHEN
		mp.setRootPageId(page.NewId(1, 10))

		// THEN
		assert.Greater(t, mp.bufPage.ModifyCount(), before)
	})

	t.Run("他フィールド (leafPageCount, height) への書き込みと独立に保持される", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage(t)

		// WHEN
		mp.setRootPageId(page.NewId(0xAA, 0xBB))
		mp.setLeafPageCount(100)
		mp.setHeight(5)

		// THEN
		assert.Equal(t, page.NewId(0xAA, 0xBB), mp.rootPageId())
		assert.Equal(t, uint64(100), mp.leafPageCount())
		assert.Equal(t, uint64(5), mp.height())
	})
}

func TestMetaPageSetLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を上書きできる", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage(t)
		mp.setLeafPageCount(10)

		// WHEN
		mp.setLeafPageCount(20)

		// THEN
		assert.Equal(t, uint64(20), mp.leafPageCount())
	})
}

func TestMetaPageSetHeight(t *testing.T) {
	t.Run("高さを上書きできる", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage(t)
		mp.setHeight(1)

		// WHEN
		mp.setHeight(5)

		// THEN
		assert.Equal(t, uint64(5), mp.height())
	})
}

// newTestMetaPage はテスト用のメタページを作成する
func newTestMetaPage(t *testing.T) *metaPage {
	t.Helper()
	pool := buffer.NewPool(page.Size, newTestRedoBuffer(t), nil)
	bufPage, err := pool.AddPage(page.NewId(0, 0))
	if err != nil {
		panic(err)
	}
	return newMetaPage(bufPage)
}

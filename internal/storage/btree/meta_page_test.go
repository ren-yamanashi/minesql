package btree

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestMetaPageRootPageId(t *testing.T) {
	t.Run("設定したルートページ ID を読み取れる", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage()
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
		mp := newTestMetaPage()
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
		mp := newTestMetaPage()
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
		mp := newTestMetaPage()
		mp.setRootPageId(page.NewId(1, 10))

		// WHEN
		mp.setRootPageId(page.NewId(2, 20))

		// THEN
		assert.Equal(t, page.NewId(2, 20), mp.rootPageId())
	})

	t.Run("他フィールド (leafPageCount, height) への書き込みと独立に保持される", func(t *testing.T) {
		// GIVEN
		mp := newTestMetaPage()

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
		mp := newTestMetaPage()
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
		mp := newTestMetaPage()
		mp.setHeight(1)

		// WHEN
		mp.setHeight(5)

		// THEN
		assert.Equal(t, uint64(5), mp.height())
	})
}

// newTestMetaPage はテスト用のメタページを作成する
func newTestMetaPage() *metaPage {
	data := make([]byte, page.Size)
	pg, _ := page.NewPage(data)
	return newMetaPage(pg)
}

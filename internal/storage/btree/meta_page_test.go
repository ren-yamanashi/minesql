package btree

import (
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMetaPage(t *testing.T) {
	t.Run("metaPage インスタンスが生成される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)

		// WHEN
		mp := newMetaPage(data)

		// THEN
		assert.NotNil(t, mp)
		assert.Equal(t, data, mp.data)
	})

	t.Run("ルートページ ID が正しく読み取れる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)
		expectedPageId := page.NewPageId(page.FileId(1), page.PageNumber(42))
		mp.setRootPageId(expectedPageId)

		// WHEN
		rootPageId := mp.rootPageId()

		// THEN
		assert.Equal(t, expectedPageId, rootPageId)
	})

	t.Run("ルートページ ID が正しく設定できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)
		expectedPageId := page.NewPageId(page.FileId(2), page.PageNumber(99))

		// WHEN
		mp.setRootPageId(expectedPageId)

		// THEN
		assert.Equal(t, expectedPageId, mp.rootPageId())
	})

	t.Run("ルートページ ID の初期値はゼロ値", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)

		// WHEN
		rootPageId := mp.rootPageId()

		// THEN
		assert.Equal(t, page.PageId{}, rootPageId)
	})

	t.Run("リーフページ数の初期値は 0", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)

		// WHEN
		count := mp.leafPageCount()

		// THEN
		assert.Equal(t, uint64(0), count)
	})

	t.Run("リーフページ数を設定・取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)

		// WHEN
		mp.setLeafPageCount(42)

		// THEN
		assert.Equal(t, uint64(42), mp.leafPageCount())
	})

	t.Run("高さの初期値は 0", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)

		// WHEN
		h := mp.height()

		// THEN
		assert.Equal(t, uint64(0), h)
	})

	t.Run("高さを設定・取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)

		// WHEN
		mp.setHeight(5)

		// THEN
		assert.Equal(t, uint64(5), mp.height())
	})

	t.Run("各フィールドが互いに干渉しない", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)
		expectedPageId := page.NewPageId(page.FileId(1), page.PageNumber(10))

		// WHEN: 全フィールドを設定
		mp.setRootPageId(expectedPageId)
		mp.setLeafPageCount(100)
		mp.setHeight(3)

		// THEN: 各フィールドが独立して正しい値を保持
		assert.Equal(t, expectedPageId, mp.rootPageId())
		assert.Equal(t, uint64(100), mp.leafPageCount())
		assert.Equal(t, uint64(3), mp.height())
	})

	t.Run("フィールドを上書きできる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := newMetaPage(data)
		mp.setLeafPageCount(10)
		mp.setHeight(2)

		// WHEN: 値を上書き
		mp.setLeafPageCount(20)
		mp.setHeight(4)

		// THEN
		assert.Equal(t, uint64(20), mp.leafPageCount())
		assert.Equal(t, uint64(4), mp.height())
	})
}

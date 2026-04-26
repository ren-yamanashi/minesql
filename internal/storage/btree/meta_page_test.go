package btree

import (
	"encoding/binary"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreateMetaPage(t *testing.T) {
	t.Run("各フィールドの初期値がゼロ値になる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)

		// WHEN
		mp := createMetaPage(page.NewPage(data))

		// THEN
		assert.Equal(t, page.PageId{}, mp.rootPageId())
		assert.Equal(t, uint64(0), mp.leafPageCount())
		assert.Equal(t, uint64(0), mp.height())
	})
}

func TestNewMetaPage(t *testing.T) {
	t.Run("metaPage インスタンスが生成される", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)

		// WHEN
		mp := newMetaPage(page.NewPage(data))

		// THEN
		assert.NotNil(t, mp)
	})
}

func TestMetaPageRootPageId(t *testing.T) {
	t.Run("ルートページ ID を設定・取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := createMetaPage(page.NewPage(data))
		expectedPageId := page.NewPageId(page.FileId(2), page.PageNumber(99))

		// WHEN
		mp.setRootPageId(expectedPageId)

		// THEN
		assert.Equal(t, expectedPageId, mp.rootPageId())
	})

	t.Run("初期値はゼロ値", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := createMetaPage(page.NewPage(data))

		// WHEN
		rootPageId := mp.rootPageId()

		// THEN
		assert.Equal(t, page.PageId{}, rootPageId)
	})
}

func TestMetaPageLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を設定・取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := createMetaPage(page.NewPage(data))

		// WHEN
		mp.setLeafPageCount(42)

		// THEN
		assert.Equal(t, uint64(42), mp.leafPageCount())
	})

	t.Run("初期値は 0", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := createMetaPage(page.NewPage(data))

		// WHEN
		count := mp.leafPageCount()

		// THEN
		assert.Equal(t, uint64(0), count)
	})
}

func TestMetaPageHeight(t *testing.T) {
	t.Run("高さを設定・取得できる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := createMetaPage(page.NewPage(data))

		// WHEN
		mp.setHeight(5)

		// THEN
		assert.Equal(t, uint64(5), mp.height())
	})

	t.Run("初期値は 0", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := createMetaPage(page.NewPage(data))

		// WHEN
		h := mp.height()

		// THEN
		assert.Equal(t, uint64(0), h)
	})
}

func TestMetaPageFieldIndependence(t *testing.T) {
	t.Run("各フィールドが互いに干渉しない", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := createMetaPage(page.NewPage(data))
		expectedPageId := page.NewPageId(page.FileId(1), page.PageNumber(10))

		// WHEN
		mp.setRootPageId(expectedPageId)
		mp.setLeafPageCount(100)
		mp.setHeight(3)

		// THEN
		assert.Equal(t, expectedPageId, mp.rootPageId())
		assert.Equal(t, uint64(100), mp.leafPageCount())
		assert.Equal(t, uint64(3), mp.height())
	})

	t.Run("ページヘッダーとメタデータフィールドが干渉しない", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := createMetaPage(page.NewPage(data))

		// WHEN
		mp.setRootPageId(page.NewPageId(page.FileId(1), page.PageNumber(10)))
		mp.setLeafPageCount(100)
		mp.setHeight(3)
		binary.BigEndian.PutUint32(page.NewPage(data).Header, 999)

		// THEN
		assert.Equal(t, uint32(999), binary.BigEndian.Uint32(page.NewPage(data).Header))
		assert.Equal(t, page.NewPageId(page.FileId(1), page.PageNumber(10)), mp.rootPageId())
		assert.Equal(t, uint64(100), mp.leafPageCount())
		assert.Equal(t, uint64(3), mp.height())
	})

	t.Run("フィールドを上書きできる", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 128)
		mp := createMetaPage(page.NewPage(data))
		mp.setLeafPageCount(10)
		mp.setHeight(2)

		// WHEN
		mp.setLeafPageCount(20)
		mp.setHeight(4)

		// THEN
		assert.Equal(t, uint64(20), mp.leafPageCount())
		assert.Equal(t, uint64(4), mp.height())
	})
}

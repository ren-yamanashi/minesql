package btree

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestIteratorGet(t *testing.T) {
	t.Run("現在のスロットのレコードを取得できる", func(t *testing.T) {
		// GIVEN
		bp, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		})
		bufPage, _ := bp.PageForRead(pageId)
		iter := NewIterator(bp, *bufPage, 0)

		// WHEN
		record, ok, err := iter.Get()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x10}, record.Key())
	})

	t.Run("スロット番号がレコード数以上の場合は false を返す", func(t *testing.T) {
		// GIVEN
		bp, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		})
		bufPage, _ := bp.PageForRead(pageId)
		iter := NewIterator(bp, *bufPage, 1)

		// WHEN
		_, ok, err := iter.Get()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestIteratorNext(t *testing.T) {
	t.Run("レコードを取得して次に進む", func(t *testing.T) {
		// GIVEN
		bp, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
			ln.insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))
		})
		bufPage, _ := bp.PageForRead(pageId)
		iter := NewIterator(bp, *bufPage, 0)

		// WHEN
		record1, ok1, err1 := iter.Next()
		record2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, []byte{0x10}, record1.Key())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, []byte{0x20}, record2.Key())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

}

func TestIteratorAdvance(t *testing.T) {
	t.Run("同一ページ内の次のスロットに進む", func(t *testing.T) {
		// GIVEN
		bp, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
			ln.insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))
		})
		bufPage, _ := bp.PageForRead(pageId)
		iter := NewIterator(bp, *bufPage, 0)

		// WHEN
		err := iter.Advance()

		// THEN
		assert.NoError(t, err)
		record, ok, _ := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, []byte{0x20}, record.Key())
	})
}

// setupIteratorTestPage はテスト用のバッファプールとリーフページを作成する
func setupIteratorTestPage(t *testing.T, setup func(ln *leafNode)) (*buffer.Pool, page.Id) {
	t.Helper()

	bp := buffer.NewPool(page.Size * 3)
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(0, path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(0, hf)

	pageId, err := bp.AllocatePageId(0)
	assert.NoError(t, err)

	bufPage, err := bp.AddPage(pageId)
	assert.NoError(t, err)

	ln := newLeafNode(bufPage.Page)
	ln.initialize()
	setup(ln)

	return bp, pageId
}

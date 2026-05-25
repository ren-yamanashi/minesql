package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIteratorClose(t *testing.T) {
	t.Run("作成直後のイテレータを Close できる", func(t *testing.T) {
		// GIVEN
		bp, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		})
		bufPage, _ := bp.PageForRead(pageId)
		iter := NewIterator(bp, *bufPage, 0)

		// WHEN
		iter.Close()

		// THEN
		record, ok, err := iter.Get()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x10}, record.Key())
	})

	t.Run("イテレーション途中で Close できる", func(t *testing.T) {
		// GIVEN
		bp, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
			ln.insert(1, NewRecord([]byte{0x01}, []byte{0x20}, []byte{0xBB}))
		})
		bufPage, _ := bp.PageForRead(pageId)
		iter := NewIterator(bp, *bufPage, 0)

		// WHEN
		_, _, _ = iter.Next()
		iter.Close()

		// THEN: パニックせずに Close できる
	})

	t.Run("全レコード読み取り後に Close できる", func(t *testing.T) {
		// GIVEN
		bp, pageId := setupIteratorTestPage(t, func(ln *leafNode) {
			ln.insert(0, NewRecord([]byte{0x01}, []byte{0x10}, []byte{0xAA}))
		})
		bufPage, _ := bp.PageForRead(pageId)
		iter := NewIterator(bp, *bufPage, 0)

		// WHEN
		_, _, _ = iter.Next()
		_, ok, _ := iter.Next()
		assert.False(t, ok)
		iter.Close()

		// THEN: パニックせずに Close できる
	})
}

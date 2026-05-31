package lock

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewRowLockKey(t *testing.T) {
	t.Run("RowKey から内部キーを生成する", func(t *testing.T) {
		// GIVEN
		metaPageId := page.NewId(page.FileId(1), page.PageNumber(7))
		rk := RowKey{MetaPageId: metaPageId, Key: []byte{0x01, 0x02, 0x03}}

		// WHEN
		got := newRowLockKey(rk)

		// THEN
		assert.Equal(t, metaPageId, got.metaPageId)
		assert.Equal(t, string([]byte{0x01, 0x02, 0x03}), got.keyStr)
	})

	t.Run("同じ MetaPageId と同じ Key からは同一の内部キーが生成される", func(t *testing.T) {
		// GIVEN
		metaPageId := page.NewId(page.FileId(1), page.PageNumber(7))
		a := RowKey{MetaPageId: metaPageId, Key: []byte{0x10, 0x20}}
		b := RowKey{MetaPageId: metaPageId, Key: []byte{0x10, 0x20}}

		// WHEN
		ka := newRowLockKey(a)
		kb := newRowLockKey(b)

		// THEN
		assert.Equal(t, ka, kb)
	})

	t.Run("MetaPageId が異なれば内部キーは別物になる", func(t *testing.T) {
		// GIVEN
		key := []byte{0x10, 0x20}
		a := RowKey{MetaPageId: page.NewId(page.FileId(1), page.PageNumber(7)), Key: key}
		b := RowKey{MetaPageId: page.NewId(page.FileId(1), page.PageNumber(8)), Key: key}

		// WHEN
		ka := newRowLockKey(a)
		kb := newRowLockKey(b)

		// THEN
		assert.NotEqual(t, ka, kb)
	})

	t.Run("Key が異なれば内部キーは別物になる", func(t *testing.T) {
		// GIVEN
		metaPageId := page.NewId(page.FileId(1), page.PageNumber(7))
		a := RowKey{MetaPageId: metaPageId, Key: []byte{0x10, 0x20}}
		b := RowKey{MetaPageId: metaPageId, Key: []byte{0x10, 0x21}}

		// WHEN
		ka := newRowLockKey(a)
		kb := newRowLockKey(b)

		// THEN
		assert.NotEqual(t, ka, kb)
	})
}

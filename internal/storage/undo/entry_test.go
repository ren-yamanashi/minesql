package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewEntry(t *testing.T) {
	t.Run("フィールドが正しく設定される", func(t *testing.T) {
		// GIVEN
		record := NewInsertRecord(page.FileId(1), btree.Record{[]byte("Alice")})

		// WHEN
		entry := NewEntry(lock.TrxId(10), RecordTypeInsert, record)

		// THEN
		assert.Equal(t, lock.TrxId(10), entry.TrxId())
		assert.Equal(t, RecordTypeInsert, entry.RecordType())
		assert.Equal(t, record, entry.Record())
	})

	t.Run("nil の Record で作成できる", func(t *testing.T) {
		// GIVEN / WHEN
		entry := NewEntry(lock.TrxId(0), RecordTypeInsert, nil)

		// THEN
		assert.Equal(t, lock.TrxId(0), entry.TrxId())
		assert.Nil(t, entry.Record())
	})
}

func TestEntryTrxId(t *testing.T) {
	t.Run("コンストラクタで指定した TrxId を返す", func(t *testing.T) {
		// GIVEN
		entry := NewEntry(lock.TrxId(42), RecordTypeDelete, nil)

		// WHEN
		result := entry.TrxId()

		// THEN
		assert.Equal(t, lock.TrxId(42), result)
	})
}

func TestEntryRecordType(t *testing.T) {
	t.Run("コンストラクタで指定した RecordType を返す", func(t *testing.T) {
		// GIVEN
		entry := NewEntry(lock.TrxId(1), RecordTypeUpdate, nil)

		// WHEN
		result := entry.RecordType()

		// THEN
		assert.Equal(t, RecordTypeUpdate, result)
	})
}

func TestEntryRecord(t *testing.T) {
	t.Run("コンストラクタで指定した Record を返す", func(t *testing.T) {
		// GIVEN
		record := NewDeleteRecord(page.FileId(3), btree.Record{[]byte("Bob")}, 5, NullPointer())

		// WHEN
		entry := NewEntry(lock.TrxId(1), RecordTypeDelete, record)

		// THEN
		assert.Equal(t, record, entry.Record())
	})
}

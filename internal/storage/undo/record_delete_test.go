package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewDeleteRecord(t *testing.T) {
	t.Run("フィールドが正しく設定される", func(t *testing.T) {
		// GIVEN
		record := btree.Record{[]byte("Alice"), []byte("alice@example.com")}
		rollPtr := Pointer{pageNumber: 3, offset: 64}

		// WHEN
		dr := NewDeleteRecord(page.FileId(5), record, 100, rollPtr)

		// THEN
		assert.Equal(t, page.FileId(5), dr.tableFileId)
		assert.Equal(t, record, dr.Record())
		assert.Equal(t, lock.TrxId(100), dr.prevLastTrxId)
		assert.Equal(t, rollPtr, dr.prevRollPtr)
	})

	t.Run("NullPointer で作成できる", func(t *testing.T) {
		// GIVEN
		record := btree.Record{[]byte("a")}

		// WHEN
		dr := NewDeleteRecord(page.FileId(1), record, 0, NullPointer())

		// THEN
		assert.Equal(t, NullPointer(), dr.prevRollPtr)
		assert.Equal(t, lock.TrxId(0), dr.prevLastTrxId)
	})
}

func TestDeleteRecordTableFileId(t *testing.T) {
	t.Run("コンストラクタで指定した FileId を返す", func(t *testing.T) {
		// GIVEN
		dr := NewDeleteRecord(page.FileId(5), btree.Record{[]byte("a")}, 0, NullPointer())

		// WHEN
		result := dr.TableFileId()

		// THEN
		assert.Equal(t, page.FileId(5), result)
	})
}

func TestDeleteRecordRecord(t *testing.T) {
	t.Run("コンストラクタで指定したレコードを返す", func(t *testing.T) {
		// GIVEN
		record := btree.Record{[]byte("Alice"), []byte("alice@example.com")}
		dr := NewDeleteRecord(page.FileId(5), record, 100, NullPointer())

		// WHEN
		result := dr.Record()

		// THEN
		assert.Equal(t, record, result)
	})

	t.Run("空のレコードを返す", func(t *testing.T) {
		// GIVEN
		dr := NewDeleteRecord(page.FileId(1), btree.Record{}, 0, NullPointer())

		// WHEN
		result := dr.Record()

		// THEN
		assert.Empty(t, result)
	})
}

func TestDeleteRecordSerialize(t *testing.T) {
	t.Run("シリアライズ結果を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		record := btree.Record{[]byte("Alice"), []byte("alice@example.com")}
		rollPtr := Pointer{pageNumber: 3, offset: 64}
		dr := NewDeleteRecord(page.FileId(5), record, 100, rollPtr)

		// WHEN
		buf := dr.Serialize(10, 2)

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(10), fields.trxId)
		assert.Equal(t, UndoNumber(2), fields.undoNumber)
		assert.Equal(t, RecordTypeDelete, fields.recordType)
		assert.Equal(t, lock.TrxId(100), fields.prevLastTrxId)
		assert.Equal(t, rollPtr, fields.prevRollPtr)
		assert.Equal(t, page.FileId(5), fields.tableFileId)
		assert.Len(t, fields.columnSets, 1)
		assert.Equal(t, [][]byte(record), fields.columnSets[0])
	})

	t.Run("Record interface を満たす", func(t *testing.T) {
		// GIVEN
		dr := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("a")}, 0, NullPointer())

		// WHEN
		var r Record = dr

		// THEN
		buf := r.Serialize(1, 0)
		assert.NotEmpty(t, buf)
	})

	t.Run("カラムが 1 つのレコードでシリアライズできる", func(t *testing.T) {
		// GIVEN
		record := btree.Record{[]byte("only_col")}
		dr := NewDeleteRecord(page.FileId(1), record, 50, NullPointer())

		// WHEN
		buf := dr.Serialize(1, 0)

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Len(t, fields.columnSets, 1)
		assert.Equal(t, [][]byte{[]byte("only_col")}, fields.columnSets[0])
	})

	t.Run("大きい TrxId でシリアライズできる", func(t *testing.T) {
		// GIVEN
		dr := NewDeleteRecord(page.FileId(1), btree.Record{[]byte("a")}, lock.TrxId(0xFFFFFFFF), NullPointer())

		// WHEN
		buf := dr.Serialize(lock.TrxId(0xFFFFFFFE), UndoNumber(0xFFFFFFFD))

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(0xFFFFFFFE), fields.trxId)
		assert.Equal(t, UndoNumber(0xFFFFFFFD), fields.undoNumber)
		assert.Equal(t, lock.TrxId(0xFFFFFFFF), fields.prevLastTrxId)
	})
}

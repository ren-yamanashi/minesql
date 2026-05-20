package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewDeleteRecord(t *testing.T) {
	t.Run("フィールドが正しく設定される", func(t *testing.T) {
		// GIVEN
		record := node.Record{[]byte("Alice"), []byte("alice@example.com")}
		rollPtr := Pointer{PageNumber: 3, Offset: 64}

		// WHEN
		dr := NewDeleteRecord(page.FileId(5), record, 100, rollPtr)

		// THEN
		assert.Equal(t, page.FileId(5), dr.tableFileId)
		assert.Equal(t, record, dr.Record)
		assert.Equal(t, lock.TrxId(100), dr.PrevLastTrxId)
		assert.Equal(t, rollPtr, dr.PrevRollPtr)
	})

	t.Run("NullPointer で作成できる", func(t *testing.T) {
		// GIVEN
		record := node.Record{[]byte("a")}

		// WHEN
		dr := NewDeleteRecord(page.FileId(1), record, 0, NullPointer)

		// THEN
		assert.Equal(t, NullPointer, dr.PrevRollPtr)
		assert.Equal(t, lock.TrxId(0), dr.PrevLastTrxId)
	})
}

func TestDeleteRecordSerialize(t *testing.T) {
	t.Run("シリアライズ結果を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		record := node.Record{[]byte("Alice"), []byte("alice@example.com")}
		rollPtr := Pointer{PageNumber: 3, Offset: 64}
		dr := NewDeleteRecord(page.FileId(5), record, 100, rollPtr)

		// WHEN
		buf := dr.Serialize(10, 2)

		// THEN
		fields, err := Deserialize(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(10), fields.TrxId)
		assert.Equal(t, UndoNumber(2), fields.UndoNum)
		assert.Equal(t, RecordTypeDelete, fields.RecordType)
		assert.Equal(t, lock.TrxId(100), fields.PrevLastTrxId)
		assert.Equal(t, rollPtr, fields.PrevRollPtr)
		assert.Equal(t, page.FileId(5), fields.TableFileId)
		assert.Len(t, fields.ColumnSets, 1)
		assert.Equal(t, [][]byte(record), fields.ColumnSets[0])
	})

	t.Run("Record interface を満たす", func(t *testing.T) {
		// GIVEN
		dr := NewDeleteRecord(page.FileId(1), node.Record{[]byte("a")}, 0, NullPointer)

		// WHEN
		var r Record = dr

		// THEN
		buf := r.Serialize(1, 0)
		assert.NotEmpty(t, buf)
	})

	t.Run("カラムが 1 つのレコードでシリアライズできる", func(t *testing.T) {
		// GIVEN
		record := node.Record{[]byte("only_col")}
		dr := NewDeleteRecord(page.FileId(1), record, 50, NullPointer)

		// WHEN
		buf := dr.Serialize(1, 0)

		// THEN
		fields, err := Deserialize(buf)
		assert.NoError(t, err)
		assert.Len(t, fields.ColumnSets, 1)
		assert.Equal(t, [][]byte{[]byte("only_col")}, fields.ColumnSets[0])
	})

	t.Run("大きい TrxId でシリアライズできる", func(t *testing.T) {
		// GIVEN
		dr := NewDeleteRecord(page.FileId(1), node.Record{[]byte("a")}, lock.TrxId(0xFFFFFFFF), NullPointer)

		// WHEN
		buf := dr.Serialize(lock.TrxId(0xFFFFFFFE), UndoNumber(0xFFFFFFFD))

		// THEN
		fields, err := Deserialize(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(0xFFFFFFFE), fields.TrxId)
		assert.Equal(t, UndoNumber(0xFFFFFFFD), fields.UndoNum)
		assert.Equal(t, lock.TrxId(0xFFFFFFFF), fields.PrevLastTrxId)
	})
}

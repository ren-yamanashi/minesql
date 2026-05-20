package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewInsertRecord(t *testing.T) {
	t.Run("フィールドが正しく設定される", func(t *testing.T) {
		// GIVEN
		record := node.Record{[]byte("Alice"), []byte("alice@example.com")}

		// WHEN
		ir := NewInsertRecord(page.FileId(5), record)

		// THEN
		assert.Equal(t, page.FileId(5), ir.tableFileId)
		assert.Equal(t, record, ir.Record)
		assert.Equal(t, lock.TrxId(0), ir.PrevLastTrxId)
		assert.Equal(t, NullPointer, ir.PrevRollPtr)
	})

	t.Run("空のレコードで作成できる", func(t *testing.T) {
		// GIVEN
		record := node.Record{}

		// WHEN
		ir := NewInsertRecord(page.FileId(1), record)

		// THEN
		assert.Empty(t, ir.Record)
		assert.Equal(t, lock.TrxId(0), ir.PrevLastTrxId)
		assert.Equal(t, NullPointer, ir.PrevRollPtr)
	})
}

func TestInsertRecordSerialize(t *testing.T) {
	t.Run("シリアライズ結果を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		record := node.Record{[]byte("Alice"), []byte("alice@example.com")}
		ir := NewInsertRecord(page.FileId(5), record)

		// WHEN
		buf := ir.Serialize(10, 2)

		// THEN
		fields, err := Deserialize(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(10), fields.TrxId)
		assert.Equal(t, UndoNumber(2), fields.UndoNum)
		assert.Equal(t, RecordTypeInsert, fields.RecordType)
		assert.Equal(t, lock.TrxId(0), fields.PrevLastTrxId)
		assert.Equal(t, NullPointer, fields.PrevRollPtr)
		assert.Equal(t, page.FileId(5), fields.TableFileId)
		assert.Len(t, fields.ColumnSets, 1)
		assert.Equal(t, [][]byte(record), fields.ColumnSets[0])
	})

	t.Run("Record interface を満たす", func(t *testing.T) {
		// GIVEN
		ir := NewInsertRecord(page.FileId(1), node.Record{[]byte("a")})

		// WHEN
		var r Record = ir

		// THEN
		buf := r.Serialize(1, 0)
		assert.NotEmpty(t, buf)
	})

	t.Run("カラムが 1 つのレコードでシリアライズできる", func(t *testing.T) {
		// GIVEN
		record := node.Record{[]byte("only_col")}
		ir := NewInsertRecord(page.FileId(1), record)

		// WHEN
		buf := ir.Serialize(1, 0)

		// THEN
		fields, err := Deserialize(buf)
		assert.NoError(t, err)
		assert.Len(t, fields.ColumnSets, 1)
		assert.Equal(t, [][]byte{[]byte("only_col")}, fields.ColumnSets[0])
	})

	t.Run("大きい TrxId でシリアライズできる", func(t *testing.T) {
		// GIVEN
		ir := NewInsertRecord(page.FileId(1), node.Record{[]byte("a")})

		// WHEN
		buf := ir.Serialize(lock.TrxId(0xFFFFFFFF), UndoNumber(0xFFFFFFFE))

		// THEN
		fields, err := Deserialize(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(0xFFFFFFFF), fields.TrxId)
		assert.Equal(t, UndoNumber(0xFFFFFFFE), fields.UndoNum)
	})
}

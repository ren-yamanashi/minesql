package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewInsertRecord(t *testing.T) {
	t.Run("フィールドが正しく設定される", func(t *testing.T) {
		// GIVEN
		record := btree.Record{[]byte("Alice"), []byte("alice@example.com")}

		// WHEN
		ir := NewInsertRecord(page.FileId(5), record)

		// THEN
		assert.Equal(t, page.FileId(5), ir.tableFileId)
		assert.Equal(t, record, ir.Record())
		assert.Equal(t, lock.TrxId(0), ir.prevLastTrxId)
		assert.Equal(t, NullPointer(), ir.prevRollPtr)
	})

	t.Run("空のレコードで作成できる", func(t *testing.T) {
		// GIVEN
		record := btree.Record{}

		// WHEN
		ir := NewInsertRecord(page.FileId(1), record)

		// THEN
		assert.Empty(t, ir.Record())
		assert.Equal(t, lock.TrxId(0), ir.prevLastTrxId)
		assert.Equal(t, NullPointer(), ir.prevRollPtr)
	})
}

func TestInsertRecordTableFileId(t *testing.T) {
	t.Run("コンストラクタで指定した FileId を返す", func(t *testing.T) {
		// GIVEN
		ir := NewInsertRecord(page.FileId(5), btree.Record{[]byte("a")})

		// WHEN
		result := ir.TableFileId()

		// THEN
		assert.Equal(t, page.FileId(5), result)
	})
}

func TestInsertRecordRecord(t *testing.T) {
	t.Run("コンストラクタで指定したレコードを返す", func(t *testing.T) {
		// GIVEN
		record := btree.Record{[]byte("Alice"), []byte("alice@example.com")}
		ir := NewInsertRecord(page.FileId(5), record)

		// WHEN
		result := ir.Record()

		// THEN
		assert.Equal(t, record, result)
	})

	t.Run("空のレコードを返す", func(t *testing.T) {
		// GIVEN
		ir := NewInsertRecord(page.FileId(1), btree.Record{})

		// WHEN
		result := ir.Record()

		// THEN
		assert.Empty(t, result)
	})
}

func TestInsertRecordSerialize(t *testing.T) {
	t.Run("シリアライズ結果を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		record := btree.Record{[]byte("Alice"), []byte("alice@example.com")}
		ir := NewInsertRecord(page.FileId(5), record)

		// WHEN
		buf := ir.Serialize(10, 2)

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(10), fields.trxId)
		assert.Equal(t, UndoNumber(2), fields.undoNum)
		assert.Equal(t, RecordTypeInsert, fields.recordType)
		assert.Equal(t, lock.TrxId(0), fields.prevLastTrxId)
		assert.Equal(t, NullPointer(), fields.prevRollPtr)
		assert.Equal(t, page.FileId(5), fields.tableFileId)
		assert.Len(t, fields.columnSets, 1)
		assert.Equal(t, [][]byte(record), fields.columnSets[0])
	})

	t.Run("Record interface を満たす", func(t *testing.T) {
		// GIVEN
		ir := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})

		// WHEN
		var r Record = ir

		// THEN
		buf := r.Serialize(1, 0)
		assert.NotEmpty(t, buf)
	})

	t.Run("カラムが 1 つのレコードでシリアライズできる", func(t *testing.T) {
		// GIVEN
		record := btree.Record{[]byte("only_col")}
		ir := NewInsertRecord(page.FileId(1), record)

		// WHEN
		buf := ir.Serialize(1, 0)

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Len(t, fields.columnSets, 1)
		assert.Equal(t, [][]byte{[]byte("only_col")}, fields.columnSets[0])
	})

	t.Run("大きい TrxId でシリアライズできる", func(t *testing.T) {
		// GIVEN
		ir := NewInsertRecord(page.FileId(1), btree.Record{[]byte("a")})

		// WHEN
		buf := ir.Serialize(lock.TrxId(0xFFFFFFFF), UndoNumber(0xFFFFFFFE))

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(0xFFFFFFFF), fields.trxId)
		assert.Equal(t, UndoNumber(0xFFFFFFFE), fields.undoNum)
	})
}

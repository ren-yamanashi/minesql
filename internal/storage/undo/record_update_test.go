package undo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewUpdateRecord(t *testing.T) {
	t.Run("フィールドが正しく設定される", func(t *testing.T) {
		// GIVEN
		prevRecord := btree.Record{[]byte("old_name"), []byte("old_email")}
		newRecord := btree.Record{[]byte("new_name"), []byte("new_email")}
		rollPtr := Pointer{pageNumber: 3, offset: 64}

		// WHEN
		ur := NewUpdateRecord(page.FileId(5), prevRecord, newRecord, 100, rollPtr)

		// THEN
		assert.Equal(t, page.FileId(5), ur.tableFileId)
		assert.Equal(t, prevRecord, ur.prevRecord)
		assert.Equal(t, newRecord, ur.newRecord)
		assert.Equal(t, lock.TrxId(100), ur.prevLastTrxId)
		assert.Equal(t, rollPtr, ur.prevRollPtr)
	})

	t.Run("NullPointer で作成できる", func(t *testing.T) {
		// GIVEN
		prevRecord := btree.Record{[]byte("a")}
		newRecord := btree.Record{[]byte("b")}

		// WHEN
		ur := NewUpdateRecord(page.FileId(1), prevRecord, newRecord, 0, NullPointer)

		// THEN
		assert.Equal(t, NullPointer, ur.prevRollPtr)
		assert.Equal(t, lock.TrxId(0), ur.prevLastTrxId)
	})
}

func TestUpdateRecordTableFileId(t *testing.T) {
	t.Run("コンストラクタで指定した FileId を返す", func(t *testing.T) {
		// GIVEN
		ur := NewUpdateRecord(page.FileId(5), btree.Record{[]byte("a")}, btree.Record{[]byte("b")}, 0, NullPointer)

		// WHEN
		result := ur.TableFileId()

		// THEN
		assert.Equal(t, page.FileId(5), result)
	})
}

func TestUpdateRecordSerialize(t *testing.T) {
	t.Run("シリアライズ結果を Deserialize でラウンドトリップできる", func(t *testing.T) {
		// GIVEN
		prevRecord := btree.Record{[]byte("old_name"), []byte("old_email")}
		newRecord := btree.Record{[]byte("new_name"), []byte("new_email")}
		rollPtr := Pointer{pageNumber: 3, offset: 64}
		ur := NewUpdateRecord(page.FileId(5), prevRecord, newRecord, 100, rollPtr)

		// WHEN
		buf := ur.serialize(10, 2)

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(10), fields.TrxId)
		assert.Equal(t, undoNumber(2), fields.UndoNum)
		assert.Equal(t, RecordTypeUpdate, fields.RecordType)
		assert.Equal(t, lock.TrxId(100), fields.PrevLastTrxId)
		assert.Equal(t, rollPtr, fields.PrevRollPtr)
		assert.Equal(t, page.FileId(5), fields.TableFileId)
		assert.Len(t, fields.ColumnSets, 2)
		assert.Equal(t, [][]byte(prevRecord), fields.ColumnSets[0])
		assert.Equal(t, [][]byte(newRecord), fields.ColumnSets[1])
	})

	t.Run("Record interface を満たす", func(t *testing.T) {
		// GIVEN
		ur := NewUpdateRecord(page.FileId(1), btree.Record{[]byte("a")}, btree.Record{[]byte("b")}, 0, NullPointer)

		// WHEN
		var r Record = ur

		// THEN
		buf := r.serialize(1, 0)
		assert.NotEmpty(t, buf)
	})

	t.Run("空のカラムデータでシリアライズできる", func(t *testing.T) {
		// GIVEN
		prevRecord := btree.Record{[]byte{}}
		newRecord := btree.Record{[]byte{}}
		ur := NewUpdateRecord(page.FileId(1), prevRecord, newRecord, 0, NullPointer)

		// WHEN
		buf := ur.serialize(1, 0)

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Len(t, fields.ColumnSets, 2)
		assert.Equal(t, [][]byte{{}}, fields.ColumnSets[0])
		assert.Equal(t, [][]byte{{}}, fields.ColumnSets[1])
	})

	t.Run("大きい TrxId でシリアライズできる", func(t *testing.T) {
		// GIVEN
		ur := NewUpdateRecord(page.FileId(1), btree.Record{[]byte("a")}, btree.Record{[]byte("b")}, lock.TrxId(0xFFFFFFFF), NullPointer)

		// WHEN
		buf := ur.serialize(lock.TrxId(0xFFFFFFFE), undoNumber(0xFFFFFFFD))

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(0xFFFFFFFE), fields.TrxId)
		assert.Equal(t, undoNumber(0xFFFFFFFD), fields.UndoNum)
		assert.Equal(t, lock.TrxId(0xFFFFFFFF), fields.PrevLastTrxId)
	})
}

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
		ur := NewUpdateRecord(page.FileId(1), prevRecord, newRecord, 0, NullPointer())

		// THEN
		assert.Equal(t, NullPointer(), ur.prevRollPtr)
		assert.Equal(t, lock.TrxId(0), ur.prevLastTrxId)
	})
}

func TestUpdateRecordTableFileId(t *testing.T) {
	t.Run("コンストラクタで指定した FileId を返す", func(t *testing.T) {
		// GIVEN
		ur := NewUpdateRecord(page.FileId(5), btree.Record{[]byte("a")}, btree.Record{[]byte("b")}, 0, NullPointer())

		// WHEN
		result := ur.TableFileId()

		// THEN
		assert.Equal(t, page.FileId(5), result)
	})
}

func TestUpdateRecordPrevRecord(t *testing.T) {
	t.Run("コンストラクタで指定した更新前レコードを返す", func(t *testing.T) {
		// GIVEN
		prevRecord := btree.Record{[]byte("old_name"), []byte("old_email")}
		newRecord := btree.Record{[]byte("new_name"), []byte("new_email")}
		ur := NewUpdateRecord(page.FileId(5), prevRecord, newRecord, 100, NullPointer())

		// WHEN
		result := ur.PrevRecord()

		// THEN
		assert.Equal(t, prevRecord, result)
	})

	t.Run("空のレコードを返す", func(t *testing.T) {
		// GIVEN
		ur := NewUpdateRecord(page.FileId(1), btree.Record{}, btree.Record{[]byte("b")}, 0, NullPointer())

		// WHEN
		result := ur.PrevRecord()

		// THEN
		assert.Empty(t, result)
	})
}

func TestUpdateRecordNewRecord(t *testing.T) {
	t.Run("コンストラクタで指定した更新後レコードを返す", func(t *testing.T) {
		// GIVEN
		prevRecord := btree.Record{[]byte("old_name"), []byte("old_email")}
		newRecord := btree.Record{[]byte("new_name"), []byte("new_email")}
		ur := NewUpdateRecord(page.FileId(5), prevRecord, newRecord, 100, NullPointer())

		// WHEN
		result := ur.NewRecord()

		// THEN
		assert.Equal(t, newRecord, result)
	})

	t.Run("空のレコードを返す", func(t *testing.T) {
		// GIVEN
		ur := NewUpdateRecord(page.FileId(1), btree.Record{[]byte("a")}, btree.Record{}, 0, NullPointer())

		// WHEN
		result := ur.NewRecord()

		// THEN
		assert.Empty(t, result)
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
		buf := ur.Serialize(10, 2)

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(10), fields.trxId)
		assert.Equal(t, UndoNumber(2), fields.undoNumber)
		assert.Equal(t, RecordTypeUpdate, fields.recordType)
		assert.Equal(t, lock.TrxId(100), fields.prevLastTrxId)
		assert.Equal(t, rollPtr, fields.prevRollPtr)
		assert.Equal(t, page.FileId(5), fields.tableFileId)
		assert.Len(t, fields.columnSets, 2)
		assert.Equal(t, [][]byte(prevRecord), fields.columnSets[0])
		assert.Equal(t, [][]byte(newRecord), fields.columnSets[1])
	})

	t.Run("Record interface を満たす", func(t *testing.T) {
		// GIVEN
		ur := NewUpdateRecord(page.FileId(1), btree.Record{[]byte("a")}, btree.Record{[]byte("b")}, 0, NullPointer())

		// WHEN
		var r Record = ur

		// THEN
		buf := r.Serialize(1, 0)
		assert.NotEmpty(t, buf)
	})

	t.Run("空のカラムデータでシリアライズできる", func(t *testing.T) {
		// GIVEN
		prevRecord := btree.Record{[]byte{}}
		newRecord := btree.Record{[]byte{}}
		ur := NewUpdateRecord(page.FileId(1), prevRecord, newRecord, 0, NullPointer())

		// WHEN
		buf := ur.Serialize(1, 0)

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Len(t, fields.columnSets, 2)
		assert.Equal(t, [][]byte{{}}, fields.columnSets[0])
		assert.Equal(t, [][]byte{{}}, fields.columnSets[1])
	})

	t.Run("大きい TrxId でシリアライズできる", func(t *testing.T) {
		// GIVEN
		ur := NewUpdateRecord(page.FileId(1), btree.Record{[]byte("a")}, btree.Record{[]byte("b")}, lock.TrxId(0xFFFFFFFF), NullPointer())

		// WHEN
		buf := ur.Serialize(lock.TrxId(0xFFFFFFFE), UndoNumber(0xFFFFFFFD))

		// THEN
		fields, err := DeserializeFields(buf)
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(0xFFFFFFFE), fields.trxId)
		assert.Equal(t, UndoNumber(0xFFFFFFFD), fields.undoNumber)
		assert.Equal(t, lock.TrxId(0xFFFFFFFF), fields.prevLastTrxId)
	})
}

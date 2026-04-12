package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializeUndoRecord(t *testing.T) {
	t.Run("INSERT レコードをシリアライズできる", func(t *testing.T) {
		// GIVEN
		columns := [][]byte{[]byte("a"), []byte("Alice")}

		// WHEN
		buf := SerializeUndoRecord(UndoRecordFields{
			TrxId:            1,
			UndoNo:           0,
			RecordType:       UndoInsert,
			PrevLastModified: 0,
			PrevRollPtr:      NullUndoPtr,
			TableName:        "users",
			ColumnSets:       [][][]byte{columns},
		})

		// THEN
		assert.True(t, len(buf) > undoRecordHeaderSize)
	})

	t.Run("UPDATE_INPLACE レコードで 2 セットのカラムデータをシリアライズできる", func(t *testing.T) {
		// GIVEN
		prevCols := [][]byte{[]byte("a"), []byte("Alice")}
		newCols := [][]byte{[]byte("a"), []byte("Bob")}

		// WHEN
		buf := SerializeUndoRecord(UndoRecordFields{
			TrxId:            1,
			UndoNo:           0,
			RecordType:       UndoUpdateInplace,
			PrevLastModified: 0,
			PrevRollPtr:      NullUndoPtr,
			TableName:        "users",
			ColumnSets:       [][][]byte{prevCols, newCols},
		})

		// THEN
		assert.True(t, len(buf) > undoRecordHeaderSize)
	})
}

func TestDeserializeUndoRecord(t *testing.T) {
	t.Run("INSERT レコードをデシリアライズできる", func(t *testing.T) {
		// GIVEN
		columns := [][]byte{[]byte("a"), []byte("Alice")}
		buf := SerializeUndoRecord(UndoRecordFields{
			TrxId:            42,
			UndoNo:           3,
			RecordType:       UndoInsert,
			PrevLastModified: 0,
			PrevRollPtr:      NullUndoPtr,
			TableName:        "users",
			ColumnSets:       [][][]byte{columns},
		})

		// WHEN
		f, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(42), f.TrxId)
		assert.Equal(t, uint64(3), f.UndoNo)
		assert.Equal(t, UndoInsert, f.RecordType)
		assert.Equal(t, TrxId(0), f.PrevLastModified)
		assert.True(t, f.PrevRollPtr.IsNull())
		assert.Equal(t, "users", f.TableName)
		assert.Equal(t, 1, len(f.ColumnSets))
		assert.Equal(t, []byte("a"), f.ColumnSets[0][0])
		assert.Equal(t, []byte("Alice"), f.ColumnSets[0][1])
	})

	t.Run("DELETE レコードをデシリアライズできる", func(t *testing.T) {
		// GIVEN
		columns := [][]byte{[]byte("b"), []byte("Bob")}
		prevRollPtr := UndoPtr{PageNumber: 2, Offset: 37}
		buf := SerializeUndoRecord(UndoRecordFields{
			TrxId:            10,
			UndoNo:           1,
			RecordType:       UndoDelete,
			PrevLastModified: 99,
			PrevRollPtr:      prevRollPtr,
			TableName:        "users",
			ColumnSets:       [][][]byte{columns},
		})

		// WHEN
		f, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), f.TrxId)
		assert.Equal(t, UndoDelete, f.RecordType)
		assert.Equal(t, TrxId(99), f.PrevLastModified)
		assert.Equal(t, prevRollPtr, f.PrevRollPtr)
		assert.Equal(t, "users", f.TableName)
		assert.Equal(t, 1, len(f.ColumnSets))
		assert.Equal(t, []byte("b"), f.ColumnSets[0][0])
		assert.Equal(t, []byte("Bob"), f.ColumnSets[0][1])
	})

	t.Run("UPDATE_INPLACE レコードで 2 セットのカラムデータをデシリアライズできる", func(t *testing.T) {
		// GIVEN
		prevCols := [][]byte{[]byte("a"), []byte("Alice")}
		newCols := [][]byte{[]byte("a"), []byte("Bob")}
		buf := SerializeUndoRecord(UndoRecordFields{
			TrxId:            5,
			UndoNo:           2,
			RecordType:       UndoUpdateInplace,
			PrevLastModified: 0,
			PrevRollPtr:      NullUndoPtr,
			TableName:        "users",
			ColumnSets:       [][][]byte{prevCols, newCols},
		})

		// WHEN
		f, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, UndoUpdateInplace, f.RecordType)
		assert.Equal(t, "users", f.TableName)
		assert.Equal(t, 2, len(f.ColumnSets))
		assert.Equal(t, []byte("Alice"), f.ColumnSets[0][1])
		assert.Equal(t, []byte("Bob"), f.ColumnSets[1][1])
	})

	t.Run("データが不足している場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		buf := make([]byte, undoRecordHeaderSize-1)

		// WHEN
		_, err := DeserializeUndoRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidUndoRecord)
	})

	t.Run("DataLen に対してデータが不足している場合はエラーを返す", func(t *testing.T) {
		// GIVEN: 正常なレコードをシリアライズし、データ部分を途中で切る
		columns := [][]byte{[]byte("a"), []byte("Alice")}
		buf := SerializeUndoRecord(UndoRecordFields{
			TrxId:            1,
			UndoNo:           0,
			RecordType:       UndoInsert,
			PrevLastModified: 0,
			PrevRollPtr:      NullUndoPtr,
			TableName:        "users",
			ColumnSets:       [][][]byte{columns},
		})
		truncated := buf[:undoRecordHeaderSize+2]

		// WHEN
		_, err := DeserializeUndoRecord(truncated)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidUndoRecord)
	})
}

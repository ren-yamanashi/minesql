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
		buf := SerializeUndoRecord(1, 0, UndoInsert, "users", columns)

		// THEN
		assert.True(t, len(buf) > undoRecordHeaderSize)
	})

	t.Run("UPDATE_INPLACE レコードで 2 セットのカラムデータをシリアライズできる", func(t *testing.T) {
		// GIVEN
		prevCols := [][]byte{[]byte("a"), []byte("Alice")}
		newCols := [][]byte{[]byte("a"), []byte("Bob")}

		// WHEN
		buf := SerializeUndoRecord(1, 0, UndoUpdateInplace, "users", prevCols, newCols)

		// THEN
		assert.True(t, len(buf) > undoRecordHeaderSize)
	})
}

func TestDeserializeUndoRecord(t *testing.T) {
	t.Run("INSERT レコードをデシリアライズできる", func(t *testing.T) {
		// GIVEN
		columns := [][]byte{[]byte("a"), []byte("Alice")}
		buf := SerializeUndoRecord(42, 3, UndoInsert, "users", columns)

		// WHEN
		trxId, undoNo, recordType, tableName, columnSets, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(42), trxId)
		assert.Equal(t, uint64(3), undoNo)
		assert.Equal(t, UndoInsert, recordType)
		assert.Equal(t, "users", tableName)
		assert.Equal(t, 1, len(columnSets))
		assert.Equal(t, []byte("a"), columnSets[0][0])
		assert.Equal(t, []byte("Alice"), columnSets[0][1])
	})

	t.Run("DELETE レコードをデシリアライズできる", func(t *testing.T) {
		// GIVEN
		columns := [][]byte{[]byte("b"), []byte("Bob")}
		buf := SerializeUndoRecord(10, 1, UndoDelete, "users", columns)

		// WHEN
		trxId, _, recordType, tableName, columnSets, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), trxId)
		assert.Equal(t, UndoDelete, recordType)
		assert.Equal(t, "users", tableName)
		assert.Equal(t, 1, len(columnSets))
		assert.Equal(t, []byte("b"), columnSets[0][0])
		assert.Equal(t, []byte("Bob"), columnSets[0][1])
	})

	t.Run("UPDATE_INPLACE レコードで 2 セットのカラムデータをデシリアライズできる", func(t *testing.T) {
		// GIVEN
		prevCols := [][]byte{[]byte("a"), []byte("Alice")}
		newCols := [][]byte{[]byte("a"), []byte("Bob")}
		buf := SerializeUndoRecord(5, 2, UndoUpdateInplace, "users", prevCols, newCols)

		// WHEN
		_, _, recordType, tableName, columnSets, err := DeserializeUndoRecord(buf)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, UndoUpdateInplace, recordType)
		assert.Equal(t, "users", tableName)
		assert.Equal(t, 2, len(columnSets))
		assert.Equal(t, []byte("Alice"), columnSets[0][1])
		assert.Equal(t, []byte("Bob"), columnSets[1][1])
	})

	t.Run("データが不足している場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		buf := make([]byte, undoRecordHeaderSize-1)

		// WHEN
		_, _, _, _, _, err := DeserializeUndoRecord(buf)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidUndoRecord)
	})

	t.Run("DataLen に対してデータが不足している場合はエラーを返す", func(t *testing.T) {
		// GIVEN: 正常なレコードをシリアライズし、データ部分を途中で切る
		columns := [][]byte{[]byte("a"), []byte("Alice")}
		buf := SerializeUndoRecord(1, 0, UndoInsert, "users", columns)
		truncated := buf[:undoRecordHeaderSize+2]

		// WHEN
		_, _, _, _, _, err := DeserializeUndoRecord(truncated)

		// THEN
		assert.ErrorIs(t, err, ErrInvalidUndoRecord)
	})
}

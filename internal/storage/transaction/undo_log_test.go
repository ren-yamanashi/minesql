package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewUndoLog(t *testing.T) {
	t.Run("空の UndoLog が生成される", func(t *testing.T) {
		// WHEN
		undoLog := NewUndoLog()

		// THEN
		assert.NotNil(t, undoLog)
		assert.Equal(t, 0, len(undoLog.GetRecords(1)))
	})
}

func TestAppend(t *testing.T) {
	t.Run("レコードが追加される", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()

		// WHEN
		undoLog.Append(1, &mockLogRecord{id: 1})
		undoLog.Append(1, &mockLogRecord{id: 2})

		// THEN
		records := undoLog.GetRecords(1)
		assert.Equal(t, 2, len(records))
	})

	t.Run("異なる trxId のレコードは分離して管理される", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()

		// WHEN
		undoLog.Append(1, &mockLogRecord{id: 1})
		undoLog.Append(2, &mockLogRecord{id: 2})
		undoLog.Append(1, &mockLogRecord{id: 3})

		// THEN
		assert.Equal(t, 2, len(undoLog.GetRecords(1)))
		assert.Equal(t, 1, len(undoLog.GetRecords(2)))
	})
}

func TestGetRecords(t *testing.T) {
	t.Run("存在しない trxId は nil を返す", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()

		// WHEN
		records := undoLog.GetRecords(999)

		// THEN
		assert.Nil(t, records)
	})

	t.Run("追加した順序でレコードが返される", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		r1 := &mockLogRecord{id: 1}
		r2 := &mockLogRecord{id: 2}
		r3 := &mockLogRecord{id: 3}
		undoLog.Append(1, r1)
		undoLog.Append(1, r2)
		undoLog.Append(1, r3)

		// WHEN
		records := undoLog.GetRecords(1)

		// THEN
		assert.Equal(t, r1, records[0])
		assert.Equal(t, r2, records[1])
		assert.Equal(t, r3, records[2])
	})
}

func TestDiscard(t *testing.T) {
	t.Run("指定した trxId のレコードが破棄される", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		undoLog.Append(1, &mockLogRecord{id: 1})
		undoLog.Append(2, &mockLogRecord{id: 2})

		// WHEN
		undoLog.Discard(1)

		// THEN
		assert.Nil(t, undoLog.GetRecords(1))
		assert.Equal(t, 1, len(undoLog.GetRecords(2)))
	})

	t.Run("存在しない trxId を Discard してもエラーにならない", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()

		// WHEN / THEN: パニックしない
		undoLog.Discard(999)
	})
}

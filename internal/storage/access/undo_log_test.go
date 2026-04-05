package access

import (
	"errors"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/lock"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockLogRecord struct {
	undone bool
}

func (m *mockLogRecord) Undo(_ *buffer.BufferPool, _ lock.TrxId, _ *lock.Manager) error {
	m.undone = true
	return nil
}

type failingLogRecord struct{}

func (f *failingLogRecord) Undo(_ *buffer.BufferPool, _ lock.TrxId, _ *lock.Manager) error {
	return errors.New("undo failed")
}

func TestNewUndoLog(t *testing.T) {
	t.Run("空の UndoLog が生成される", func(t *testing.T) {
		// GIVEN / WHEN
		undoLog := NewUndoLog()

		// THEN
		assert.NotNil(t, undoLog)
		assert.Nil(t, undoLog.GetRecords(1))
	})
}

func TestUndoLogAppend(t *testing.T) {
	t.Run("レコードを追加できる", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()

		// WHEN
		undoLog.Append(1, &mockLogRecord{})
		undoLog.Append(1, &mockLogRecord{})

		// THEN
		assert.Equal(t, 2, len(undoLog.GetRecords(1)))
	})

	t.Run("異なるトランザクションに独立して追加できる", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()

		// WHEN
		undoLog.Append(1, &mockLogRecord{})
		undoLog.Append(2, &mockLogRecord{})
		undoLog.Append(2, &mockLogRecord{})

		// THEN
		assert.Equal(t, 1, len(undoLog.GetRecords(1)))
		assert.Equal(t, 2, len(undoLog.GetRecords(2)))
	})
}

func TestGetRecords(t *testing.T) {
	t.Run("存在しないトランザクション ID は nil を返す", func(t *testing.T) {
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
		r1 := &mockLogRecord{}
		r2 := &mockLogRecord{}
		r3 := &mockLogRecord{}
		undoLog.Append(1, r1)
		undoLog.Append(1, r2)
		undoLog.Append(1, r3)

		// WHEN
		records := undoLog.GetRecords(1)

		// THEN
		assert.Equal(t, 3, len(records))
		assert.Same(t, r1, records[0])
		assert.Same(t, r2, records[1])
		assert.Same(t, r3, records[2])
	})
}

func TestPopLast(t *testing.T) {
	t.Run("最後のレコードが削除される", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		r1 := &mockLogRecord{}
		r2 := &mockLogRecord{}
		undoLog.Append(1, r1)
		undoLog.Append(1, r2)

		// WHEN
		undoLog.PopLast(1)

		// THEN
		records := undoLog.GetRecords(1)
		assert.Equal(t, 1, len(records))
		assert.Same(t, r1, records[0])
	})

	t.Run("空のトランザクションに対して PopLast してもパニックしない", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()

		// WHEN / THEN
		assert.NotPanics(t, func() {
			undoLog.PopLast(1)
		})
	})

	t.Run("他のトランザクションのレコードに影響しない", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		undoLog.Append(1, &mockLogRecord{})
		undoLog.Append(2, &mockLogRecord{})
		undoLog.Append(2, &mockLogRecord{})

		// WHEN
		undoLog.PopLast(2)

		// THEN
		assert.Equal(t, 1, len(undoLog.GetRecords(1)))
		assert.Equal(t, 1, len(undoLog.GetRecords(2)))
	})
}

func TestDiscard(t *testing.T) {
	t.Run("指定したトランザクションのログが破棄される", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		undoLog.Append(1, &mockLogRecord{})
		undoLog.Append(1, &mockLogRecord{})

		// WHEN
		undoLog.Discard(1)

		// THEN
		assert.Nil(t, undoLog.GetRecords(1))
	})

	t.Run("他のトランザクションのログに影響しない", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		undoLog.Append(1, &mockLogRecord{})
		undoLog.Append(2, &mockLogRecord{})

		// WHEN
		undoLog.Discard(1)

		// THEN
		assert.Nil(t, undoLog.GetRecords(1))
		assert.Equal(t, 1, len(undoLog.GetRecords(2)))
	})

	t.Run("存在しないトランザクション ID を Discard してもパニックしない", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()

		// WHEN / THEN
		assert.NotPanics(t, func() {
			undoLog.Discard(999)
		})
	})
}

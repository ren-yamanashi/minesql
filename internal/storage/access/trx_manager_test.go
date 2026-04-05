package access

import (
	"minesql/internal/storage/lock"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewManager(t *testing.T) {
	t.Run("空の Manager が生成される", func(t *testing.T) {
		// GIVEN / WHEN
		undoLog := NewUndoLog()
		manager := NewManager(undoLog, lock.NewManager(5000))

		// THEN
		assert.NotNil(t, manager)
		assert.Equal(t, 0, len(manager.Transactions))
	})
}

func TestManagerBegin(t *testing.T) {
	t.Run("トランザクションが存在しない場合は TrxId 1 が割り当てられる", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		manager := NewManager(undoLog, lock.NewManager(5000))

		// WHEN
		id := manager.Begin()

		// THEN
		assert.Equal(t, TrxId(1), id)
		assert.Equal(t, StateActive, manager.Transactions[id])
	})

	t.Run("連続して Begin すると単調増加する", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		manager := NewManager(undoLog, lock.NewManager(5000))

		// WHEN
		id1 := manager.Begin()
		id2 := manager.Begin()
		id3 := manager.Begin()

		// THEN
		assert.Equal(t, TrxId(1), id1)
		assert.Equal(t, TrxId(2), id2)
		assert.Equal(t, TrxId(3), id3)
	})
}

func TestManagerCommit(t *testing.T) {
	t.Run("Commit すると状態が INACTIVE になる", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		manager := NewManager(undoLog, lock.NewManager(5000))
		trxId := manager.Begin()

		// WHEN
		manager.Commit(trxId)

		// THEN
		assert.Equal(t, StateInactive, manager.Transactions[trxId])
	})

	t.Run("Commit すると Undo ログが破棄される", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		manager := NewManager(undoLog, lock.NewManager(5000))
		trxId := manager.Begin()
		undoLog.Append(trxId, &mockLogRecord{})
		undoLog.Append(trxId, &mockLogRecord{})
		assert.Equal(t, 2, len(undoLog.GetRecords(trxId)))

		// WHEN
		manager.Commit(trxId)

		// THEN
		assert.Nil(t, undoLog.GetRecords(trxId))
	})
}

func TestManagerRollback(t *testing.T) {
	t.Run("Rollback すると Undo ログが逆順に適用される", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		manager := NewManager(undoLog, lock.NewManager(5000))
		trxId := manager.Begin()
		r1 := &mockLogRecord{}
		r2 := &mockLogRecord{}
		r3 := &mockLogRecord{}
		undoLog.Append(trxId, r1)
		undoLog.Append(trxId, r2)
		undoLog.Append(trxId, r3)

		// WHEN
		err := manager.Rollback(nil, trxId)

		// THEN
		assert.NoError(t, err)
		assert.True(t, r1.undone)
		assert.True(t, r2.undone)
		assert.True(t, r3.undone)
	})

	t.Run("Rollback すると状態が INACTIVE になり Undo ログが破棄される", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		manager := NewManager(undoLog, lock.NewManager(5000))
		trxId := manager.Begin()
		undoLog.Append(trxId, &mockLogRecord{})

		// WHEN
		err := manager.Rollback(nil, trxId)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, StateInactive, manager.Transactions[trxId])
		assert.Nil(t, undoLog.GetRecords(trxId))
	})

	t.Run("Undo がエラーを返した場合、Rollback もエラーを返す", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		manager := NewManager(undoLog, lock.NewManager(5000))
		trxId := manager.Begin()
		undoLog.Append(trxId, &failingLogRecord{})

		// WHEN
		err := manager.Rollback(nil, trxId)

		// THEN
		assert.Error(t, err)
	})

	t.Run("他のトランザクションの Undo ログには影響しない", func(t *testing.T) {
		// GIVEN
		undoLog := NewUndoLog()
		manager := NewManager(undoLog, lock.NewManager(5000))
		trx1 := manager.Begin()
		trx2 := manager.Begin()
		undoLog.Append(trx1, &mockLogRecord{})
		r2 := &mockLogRecord{}
		undoLog.Append(trx2, r2)

		// WHEN
		err := manager.Rollback(nil, trx1)

		// THEN
		assert.NoError(t, err)
		assert.False(t, r2.undone)
		assert.Equal(t, 1, len(undoLog.GetRecords(trx2)))
	})
}

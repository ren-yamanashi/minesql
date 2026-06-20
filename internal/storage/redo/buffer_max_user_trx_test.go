package redo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

func TestMaxUserTrxId(t *testing.T) {
	t.Run("レコードが 1 件もない場合は 0 を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		maxId, err := buf.MaxUserTrxId()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(0), maxId)
	})

	t.Run("ユーザートランザクションが複数ある場合は最大値を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, err := buf.AppendCommit(lock.TrxId(3))
		assert.NoError(t, err)
		_, err = buf.AppendCommit(lock.TrxId(100))
		assert.NoError(t, err)
		_, err = buf.AppendCommit(lock.TrxId(50))
		assert.NoError(t, err)
		assert.NoError(t, buf.Flush())

		// WHEN
		maxId, err := buf.MaxUserTrxId()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(100), maxId)
	})

	t.Run("システム予約トランザクション ID は除外する", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, err := buf.AppendCommit(lock.TrxId(7))
		assert.NoError(t, err)
		_, err = buf.AppendCommit(lock.SystemReservedTrxId)
		assert.NoError(t, err)
		assert.NoError(t, buf.Flush())

		// WHEN
		maxId, err := buf.MaxUserTrxId()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(7), maxId)
	})

	t.Run("Purge 予約トランザクション ID は除外する", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, err := buf.AppendCommit(lock.TrxId(7))
		assert.NoError(t, err)
		_, err = buf.AppendCommit(lock.PurgeReservedTrxId)
		assert.NoError(t, err)
		assert.NoError(t, buf.Flush())

		// WHEN
		maxId, err := buf.MaxUserTrxId()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(7), maxId)
	})

	t.Run("予約 trxId のみの場合は 0 を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, err := buf.AppendCommit(lock.SystemReservedTrxId)
		assert.NoError(t, err)
		_, err = buf.AppendCommit(lock.PurgeReservedTrxId)
		assert.NoError(t, err)
		assert.NoError(t, buf.Flush())

		// WHEN
		maxId, err := buf.MaxUserTrxId()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(0), maxId)
	})
}

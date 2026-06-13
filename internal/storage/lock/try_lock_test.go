package lock

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestManagerTryLock(t *testing.T) {
	t.Run("競合がなければ即時付与され待機ハンドルは nil", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testRowKey(1, 0)

		// WHEN
		granted, handle := m.TryLock(1, pos, Exclusive)

		// THEN
		assert.True(t, granted)
		assert.Nil(t, handle)
	})

	t.Run("既にロックを保持している場合は即時付与される", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testRowKey(1, 0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		granted, handle := m.TryLock(1, pos, Exclusive)

		// THEN
		assert.True(t, granted)
		assert.Nil(t, handle)
	})

	t.Run("競合する場合は granted=false と待機ハンドルを返す", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testRowKey(1, 0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		granted, handle := m.TryLock(2, pos, Exclusive)

		// THEN
		assert.False(t, granted)
		assert.NotNil(t, handle)
	})

	t.Run("待機ハンドルで待機しロック解放後に付与される", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testRowKey(1, 0)
		_ = m.Lock(1, pos, Exclusive)
		granted, handle := m.TryLock(2, pos, Exclusive)
		assert.False(t, granted)

		var wg sync.WaitGroup
		var waitErr error
		wg.Go(func() {
			waitErr = handle.Wait()
		})

		// WHEN
		time.Sleep(10 * time.Millisecond)
		m.Release(1)
		wg.Wait()

		// THEN
		assert.NoError(t, waitErr)
	})

	t.Run("待機ハンドルの Wait がタイムアウトする", func(t *testing.T) {
		// GIVEN
		m := newManagerWithShortTimeout()
		pos := testRowKey(1, 0)
		_ = m.Lock(1, pos, Exclusive)
		granted, handle := m.TryLock(2, pos, Exclusive)
		assert.False(t, granted)

		// WHEN
		err := handle.Wait()

		// THEN
		assert.ErrorIs(t, err, ErrTimeout)
	})

	t.Run("即時判定とキュー登録が原子的に行われ待機キューは FIFO 順になる", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testRowKey(1, 0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		g2, h2 := m.TryLock(2, pos, Exclusive)
		g3, h3 := m.TryLock(3, pos, Exclusive)

		// THEN
		assert.False(t, g2)
		assert.NotNil(t, h2)
		assert.False(t, g3)
		assert.NotNil(t, h3)

		m.mu.Lock()
		state := m.lockTable[newRowLockKey(pos)]
		assert.Len(t, state.waitQueue, 2)
		assert.Equal(t, TrxId(2), state.waitQueue[0].trxId)
		assert.Equal(t, TrxId(3), state.waitQueue[1].trxId)
		m.mu.Unlock()

		// 解放すると先頭の待機者 (trx2) のみが付与され、後続の Exclusive (trx3) は待機したまま
		m.Release(1)

		m.mu.Lock()
		state = m.lockTable[newRowLockKey(pos)]
		assert.Equal(t, Exclusive, state.holders[2])
		_, holds3 := state.holders[3]
		assert.False(t, holds3)
		assert.Len(t, state.waitQueue, 1)
		assert.Equal(t, TrxId(3), state.waitQueue[0].trxId)
		m.mu.Unlock()
	})

	t.Run("待機キューの先頭の Shared 待機者は解放で付与される", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testRowKey(1, 0)
		_ = m.Lock(1, pos, Exclusive)
		granted, handle := m.TryLock(2, pos, Shared)
		assert.False(t, granted)

		var wg sync.WaitGroup
		var waitErr error
		wg.Go(func() {
			waitErr = handle.Wait()
		})

		// WHEN
		time.Sleep(10 * time.Millisecond)
		m.Release(1)
		wg.Wait()

		// THEN
		assert.NoError(t, waitErr)
		m.mu.Lock()
		state := m.lockTable[newRowLockKey(pos)]
		assert.Equal(t, Shared, state.holders[2])
		m.mu.Unlock()
	})
}

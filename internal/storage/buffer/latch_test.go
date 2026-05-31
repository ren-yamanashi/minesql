package buffer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRWLatchLockShared(t *testing.T) {
	t.Run("競合なしで即座に取得できる", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()

		// WHEN
		l.LockShared()
		defer l.Unlock(LatchShared)

		// THEN
		assert.Equal(t, 1, l.sharedCnt)
		assert.False(t, l.xHeld)
		assert.False(t, l.sxHeld)
	})

	t.Run("複数の Shared が同時に保持できる", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()

		// WHEN
		l.LockShared()
		l.LockShared()
		defer l.Unlock(LatchShared)
		defer l.Unlock(LatchShared)

		// THEN
		assert.Equal(t, 2, l.sharedCnt)
	})

	t.Run("X 保持中は Shared 取得がブロックされ、解放後に取得できる", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()
		l.LockExclusive()
		acquired := atomic.Bool{}

		// WHEN
		go func() {
			l.LockShared()
			acquired.Store(true)
		}()
		time.Sleep(20 * time.Millisecond)
		blockedBeforeUnlock := acquired.Load()
		l.Unlock(LatchExclusive)

		// THEN
		assert.False(t, blockedBeforeUnlock)
		assert.Eventually(t, acquired.Load, time.Second, 5*time.Millisecond)
		l.Unlock(LatchShared)
	})
}

func TestRWLatchLockExclusive(t *testing.T) {
	t.Run("競合なしで即座に取得できる", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()

		// WHEN
		l.LockExclusive()
		defer l.Unlock(LatchExclusive)

		// THEN
		assert.True(t, l.xHeld)
		assert.Equal(t, 0, l.sharedCnt)
	})

	t.Run("Shared 保持中は Exclusive 取得がブロックされ、解放後に取得できる", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()
		l.LockShared()
		acquired := atomic.Bool{}

		// WHEN
		go func() {
			l.LockExclusive()
			acquired.Store(true)
		}()
		time.Sleep(20 * time.Millisecond)
		blockedBeforeUnlock := acquired.Load()
		l.Unlock(LatchShared)

		// THEN
		assert.False(t, blockedBeforeUnlock)
		assert.Eventually(t, acquired.Load, time.Second, 5*time.Millisecond)
		l.Unlock(LatchExclusive)
	})

	t.Run("Exclusive 待機中は後続の Shared 要求も待たされる (FIFO 公平性)", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()
		l.LockShared()
		xAcquired := atomic.Bool{}
		sAcquired := atomic.Bool{}

		// WHEN
		go func() {
			l.LockExclusive()
			xAcquired.Store(true)
		}()
		time.Sleep(20 * time.Millisecond)
		go func() {
			l.LockShared()
			sAcquired.Store(true)
		}()
		time.Sleep(20 * time.Millisecond)
		preconditionXBlocked := xAcquired.Load()
		preconditionSBlocked := sAcquired.Load()
		l.Unlock(LatchShared)

		// THEN
		assert.False(t, preconditionXBlocked)
		assert.False(t, preconditionSBlocked)
		assert.Eventually(t, xAcquired.Load, time.Second, 5*time.Millisecond)
		l.Unlock(LatchExclusive)
		assert.Eventually(t, sAcquired.Load, time.Second, 5*time.Millisecond)
		l.Unlock(LatchShared)
	})
}

func TestRWLatchLockSharedExclusive(t *testing.T) {
	t.Run("Shared 保持中でも SX を取得できる", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()
		l.LockShared()

		// WHEN
		l.LockSharedExclusive()

		// THEN
		assert.True(t, l.sxHeld)
		assert.Equal(t, 1, l.sharedCnt)
		l.Unlock(LatchSharedExclusive)
		l.Unlock(LatchShared)
	})

	t.Run("SX 同士は競合する", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()
		l.LockSharedExclusive()
		acquired := atomic.Bool{}

		// WHEN
		go func() {
			l.LockSharedExclusive()
			acquired.Store(true)
		}()
		time.Sleep(20 * time.Millisecond)
		blockedBeforeUnlock := acquired.Load()
		l.Unlock(LatchSharedExclusive)

		// THEN
		assert.False(t, blockedBeforeUnlock)
		assert.Eventually(t, acquired.Load, time.Second, 5*time.Millisecond)
		l.Unlock(LatchSharedExclusive)
	})

	t.Run("SX 保持中は Exclusive 取得がブロックされる", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()
		l.LockSharedExclusive()
		acquired := atomic.Bool{}

		// WHEN
		go func() {
			l.LockExclusive()
			acquired.Store(true)
		}()
		time.Sleep(20 * time.Millisecond)
		blockedBeforeUnlock := acquired.Load()
		l.Unlock(LatchSharedExclusive)

		// THEN
		assert.False(t, blockedBeforeUnlock)
		assert.Eventually(t, acquired.Load, time.Second, 5*time.Millisecond)
		l.Unlock(LatchExclusive)
	})
}

func TestRWLatchUnlockGrantsWaiters(t *testing.T) {
	t.Run("X 解放時に複数の待機 Shared を一括 grant する", func(t *testing.T) {
		// GIVEN
		l := NewRWLatch()
		l.LockExclusive()
		const waiters = 4
		var wg sync.WaitGroup
		wg.Add(waiters)
		for range waiters {
			go func() {
				l.LockShared()
				wg.Done()
			}()
		}
		time.Sleep(20 * time.Millisecond)

		// WHEN
		l.Unlock(LatchExclusive)
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()

		// THEN
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("複数の Shared が grant されなかった")
		}
		assert.Equal(t, waiters, l.sharedCnt)
		for range waiters {
			l.Unlock(LatchShared)
		}
	})
}

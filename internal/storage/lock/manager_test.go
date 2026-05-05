package lock

import (
	"sync"
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewManager(t *testing.T) {
	t.Run("Manager を作成できる", func(t *testing.T) {
		// WHEN
		m := NewManager()

		// THEN
		assert.NotNil(t, m)
		assert.NotNil(t, m.lockTable)
		assert.NotNil(t, m.heldLocks)
		assert.NotNil(t, m.cond)
	})
}

func TestManagerLock(t *testing.T) {
	t.Run("Shared ロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testPos(1, 0)

		// WHEN
		err := m.Lock(1, pos, Shared)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("Exclusive ロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testPos(1, 0)

		// WHEN
		err := m.Lock(1, pos, Exclusive)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一トランザクションが同一レコードに Shared を 2 回要求しても成功する", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Shared)

		// WHEN
		err := m.Lock(1, pos, Shared)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一トランザクションが Exclusive 保持中に Shared を要求しても成功する", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		err := m.Lock(1, pos, Shared)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("異なるトランザクションが同一レコードに Shared ロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Shared)

		// WHEN
		err := m.Lock(2, pos, Shared)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("異なるレコードには競合せず Exclusive ロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		_ = m.Lock(1, testPos(1, 0), Exclusive)

		// WHEN
		err := m.Lock(2, testPos(1, 1), Exclusive)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("Shared 保持中に他トランザクションが Exclusive を要求するとタイムアウトする", func(t *testing.T) {
		// GIVEN
		m := newManagerWithShortTimeout()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Shared)

		// WHEN
		err := m.Lock(2, pos, Exclusive)

		// THEN
		assert.ErrorIs(t, err, ErrTimeout)
	})

	t.Run("Exclusive 保持中に他トランザクションが Shared を要求するとタイムアウトする", func(t *testing.T) {
		// GIVEN
		m := newManagerWithShortTimeout()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		err := m.Lock(2, pos, Shared)

		// THEN
		assert.ErrorIs(t, err, ErrTimeout)
	})

	t.Run("Exclusive 保持中に他トランザクションが Exclusive を要求するとタイムアウトする", func(t *testing.T) {
		// GIVEN
		m := newManagerWithShortTimeout()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		err := m.Lock(2, pos, Exclusive)

		// THEN
		assert.ErrorIs(t, err, ErrTimeout)
	})

	t.Run("ロック解放後に待機中のトランザクションがロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Exclusive)

		var wg sync.WaitGroup
		var lockErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockErr = m.Lock(2, pos, Exclusive)
		}()

		// WHEN
		time.Sleep(10 * time.Millisecond)
		m.Release(1)
		wg.Wait()

		// THEN
		assert.NoError(t, lockErr)
	})

	t.Run("Shared→Exclusive の昇格ができる", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Shared)

		// WHEN
		err := m.Lock(1, pos, Exclusive)

		// THEN
		assert.NoError(t, err)
	})
}

func TestManagerRelease(t *testing.T) {
	t.Run("ロックを解放するとロックテーブルからエントリが削除される", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		m.Release(1)

		// THEN
		assert.Empty(t, m.lockTable)
		assert.Empty(t, m.heldLocks)
	})

	t.Run("複数レコードのロックを一括解放できる", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		_ = m.Lock(1, testPos(1, 0), Exclusive)
		_ = m.Lock(1, testPos(1, 1), Shared)

		// WHEN
		m.Release(1)

		// THEN
		assert.Empty(t, m.lockTable)
	})

	t.Run("ロックを保持していないトランザクションを解放しても問題ない", func(t *testing.T) {
		// GIVEN
		m := NewManager()

		// WHEN / THEN
		assert.NotPanics(t, func() {
			m.Release(999)
		})
	})

	t.Run("解放後に待機キューの Shared ロックが連続して付与される", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		pos := testPos(1, 0)
		_ = m.Lock(1, pos, Exclusive)

		var wg sync.WaitGroup
		var err2, err3 error
		wg.Add(2)
		go func() {
			defer wg.Done()
			err2 = m.Lock(2, pos, Shared)
		}()
		go func() {
			defer wg.Done()
			err3 = m.Lock(3, pos, Shared)
		}()

		// WHEN
		time.Sleep(10 * time.Millisecond)
		m.Release(1)
		wg.Wait()

		// THEN
		assert.NoError(t, err2)
		assert.NoError(t, err3)
	})
}

func TestManagerGrantWaitingLocks(t *testing.T) {
	t.Run("待機キュー [S, S, X] の場合 S を 2 つ付与して X で停止する", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		s := newState()
		s.waitQueue = []*request{
			{trxId: 1, mode: Shared},
			{trxId: 2, mode: Shared},
			{trxId: 3, mode: Exclusive},
		}

		// WHEN
		m.grantWaitingLocks(s)

		// THEN
		assert.Equal(t, Shared, s.holders[1])
		assert.Equal(t, Shared, s.holders[2])
		assert.Len(t, s.waitQueue, 1)
		assert.Equal(t, TrxId(3), s.waitQueue[0].trxId)
	})

	t.Run("待機キュー [X] の場合 X を付与する", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		s := newState()
		s.waitQueue = []*request{
			{trxId: 1, mode: Exclusive},
		}

		// WHEN
		m.grantWaitingLocks(s)

		// THEN
		assert.Equal(t, Exclusive, s.holders[1])
		assert.Empty(t, s.waitQueue)
	})

	t.Run("待機キュー [X, S] の場合 X を付与して S は付与しない", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		s := newState()
		s.waitQueue = []*request{
			{trxId: 1, mode: Exclusive},
			{trxId: 2, mode: Shared},
		}

		// WHEN
		m.grantWaitingLocks(s)

		// THEN
		assert.Equal(t, Exclusive, s.holders[1])
		assert.Len(t, s.waitQueue, 1)
		assert.Equal(t, TrxId(2), s.waitQueue[0].trxId)
	})

	t.Run("空の待機キューでは何もしない", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		s := newState()

		// WHEN
		m.grantWaitingLocks(s)

		// THEN
		assert.Empty(t, s.holders)
		assert.Empty(t, s.waitQueue)
	})

	t.Run("既に Shared 保持者がいる場合に待機キューの Shared を付与できる", func(t *testing.T) {
		// GIVEN
		m := NewManager()
		s := newState()
		s.holders[1] = Shared
		s.waitQueue = []*request{
			{trxId: 2, mode: Shared},
		}

		// WHEN
		m.grantWaitingLocks(s)

		// THEN
		assert.Equal(t, Shared, s.holders[2])
		assert.Empty(t, s.waitQueue)
	})
}

// testPos はテスト用の RecordPosition を作成する
func testPos(pageNum page.PageNumber, slot int) node.RecordPosition {
	return node.RecordPosition{
		PageId:  page.NewPageId(page.FileId(1), pageNum),
		SlotNum: slot,
	}
}

// newManagerWithShortTimeout はタイムアウトを短くした Manager を作成する
func newManagerWithShortTimeout() *Manager {
	m := NewManager()
	m.timeout = 50 * time.Millisecond
	return m
}

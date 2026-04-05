package lock

import (
	"minesql/internal/storage/page"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {
	t.Run("共有ロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos := slotPos(0)

		// WHEN
		err := m.Lock(1, pos, Shared)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("排他ロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos := slotPos(0)

		// WHEN
		err := m.Lock(1, pos, Exclusive)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一トランザクションが同じ行に対して共有ロックを重複取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Shared)

		// WHEN
		err := m.Lock(1, pos, Shared)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一トランザクションが排他ロックを保持中に共有ロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		err := m.Lock(1, pos, Shared)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("複数のトランザクションが同じ行の共有ロックを同時に保持できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Shared)

		// WHEN
		err := m.Lock(2, pos, Shared)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("異なる行に対して異なるトランザクションが排他ロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos1 := slotPos(0)
		pos2 := slotPos(1)
		_ = m.Lock(1, pos1, Exclusive)

		// WHEN
		err := m.Lock(2, pos2, Exclusive)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("共有ロックから排他ロックへアップグレードできる", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Shared)

		// WHEN
		err := m.Lock(1, pos, Exclusive)

		// THEN
		assert.NoError(t, err)
	})
}

func TestLockTimeout(t *testing.T) {
	t.Run("排他ロックが保持されている行への共有ロック取得はタイムアウトする", func(t *testing.T) {
		// GIVEN
		m := NewManager(50)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		err := m.Lock(2, pos, Shared)

		// THEN
		assert.ErrorIs(t, err, ErrTimeout)
	})

	t.Run("排他ロックが保持されている行への排他ロック取得はタイムアウトする", func(t *testing.T) {
		// GIVEN
		m := NewManager(50)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		err := m.Lock(2, pos, Exclusive)

		// THEN
		assert.ErrorIs(t, err, ErrTimeout)
	})

	t.Run("共有ロックが保持されている行への排他ロック取得はタイムアウトする", func(t *testing.T) {
		// GIVEN
		m := NewManager(50)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Shared)

		// WHEN
		err := m.Lock(2, pos, Exclusive)

		// THEN
		assert.ErrorIs(t, err, ErrTimeout)
	})
}

func TestReleaseAll(t *testing.T) {
	t.Run("排他ロック解放後に他のトランザクションがロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		m.ReleaseAll(1)
		err := m.Lock(2, pos, Exclusive)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("複数行のロックが一括解放される", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos1 := slotPos(0)
		pos2 := slotPos(1)
		_ = m.Lock(1, pos1, Exclusive)
		_ = m.Lock(1, pos2, Exclusive)

		// WHEN
		m.ReleaseAll(1)
		err1 := m.Lock(2, pos1, Exclusive)
		err2 := m.Lock(2, pos2, Exclusive)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
	})

	t.Run("ロックを保持していないトランザクションの ReleaseAll はパニックしない", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)

		// WHEN / THEN
		assert.NotPanics(t, func() {
			m.ReleaseAll(999)
		})
	})

	t.Run("解放後に lockTable からエントリが削除される", func(t *testing.T) {
		// GIVEN
		m := NewManager(100)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		m.ReleaseAll(1)

		// THEN
		m.mutex.Lock()
		_, exists := m.lockTable[pos]
		m.mutex.Unlock()
		assert.False(t, exists)
	})
}

func TestConcurrentLock(t *testing.T) {
	t.Run("排他ロック解放を待機しているトランザクションがロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(500)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Exclusive)

		var wg sync.WaitGroup
		var lockErr error

		// WHEN
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockErr = m.Lock(2, pos, Shared)
		}()

		time.Sleep(20 * time.Millisecond)
		m.ReleaseAll(1)
		wg.Wait()

		// THEN
		assert.NoError(t, lockErr)
	})

	t.Run("複数の共有ロック待機者が同時にロックを取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(500)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Exclusive)

		var wg sync.WaitGroup
		errs := make([]error, 3)

		// WHEN
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				errs[idx] = m.Lock(TrxId(10+idx), pos, Shared)
			}(i)
		}

		time.Sleep(20 * time.Millisecond)
		m.ReleaseAll(1)
		wg.Wait()

		// THEN
		for i, err := range errs {
			assert.NoError(t, err, "トランザクション %d がロック取得に失敗", 10+i)
		}
	})

	t.Run("FIFO 順序で排他ロック待機者にロックが付与される", func(t *testing.T) {
		// GIVEN
		m := NewManager(1000)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Exclusive)

		order := make([]TrxId, 0, 3)
		var mu sync.Mutex
		var wg sync.WaitGroup

		// WHEN: 3 つの排他ロック待機者を順番に追加
		for i := 0; i < 3; i++ {
			wg.Add(1)
			trxId := TrxId(10 + i)
			go func(id TrxId) {
				defer wg.Done()
				err := m.Lock(id, pos, Exclusive)
				if err != nil {
					return
				}
				mu.Lock()
				order = append(order, id)
				mu.Unlock()
				// 次の待機者にロックを渡すために少し待ってから解放
				time.Sleep(10 * time.Millisecond)
				m.ReleaseAll(id)
			}(trxId)
			time.Sleep(10 * time.Millisecond)
		}

		time.Sleep(10 * time.Millisecond)
		m.ReleaseAll(1)
		wg.Wait()

		// THEN
		assert.Equal(t, 3, len(order))
		assert.Equal(t, TrxId(10), order[0])
		assert.Equal(t, TrxId(11), order[1])
		assert.Equal(t, TrxId(12), order[2])
	})

	t.Run("共有ロック解放後に待機中の排他ロックが取得できる", func(t *testing.T) {
		// GIVEN
		m := NewManager(500)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Shared)
		_ = m.Lock(2, pos, Shared)

		var wg sync.WaitGroup
		var lockErr error

		// WHEN
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockErr = m.Lock(3, pos, Exclusive)
		}()

		time.Sleep(20 * time.Millisecond)
		m.ReleaseAll(1)
		m.ReleaseAll(2)
		wg.Wait()

		// THEN
		assert.NoError(t, lockErr)
	})

	t.Run("解放後も排他ロック待機者の後ろの共有ロックは付与されない (FIFO)", func(t *testing.T) {
		// GIVEN: trx1, trx2 が共有ロックを保持
		m := NewManager(200)
		pos := slotPos(0)
		err := m.Lock(1, pos, Shared)
		assert.NoError(t, err)
		err = m.Lock(2, pos, Shared)
		assert.NoError(t, err)

		var wg sync.WaitGroup
		var err3, err4 error

		// trx3 が排他ロックを待機
		wg.Add(1)
		go func() {
			defer wg.Done()
			err3 = m.Lock(3, pos, Exclusive)
		}()

		// trx4 が共有ロックを待機 (trx3 の後ろ)
		time.Sleep(20 * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err4 = m.Lock(4, pos, Shared)
		}()

		// WHEN: trx1 を解放 (trx2 はまだ保持)
		// grantWaitingLocks: trx3(exclusive) は trx2 がいるため付与不可 → break → trx4 も未処理
		time.Sleep(50 * time.Millisecond)
		m.ReleaseAll(1)

		wg.Wait()

		// THEN: trx2 がまだ保持しているため、trx3 も trx4 もタイムアウト
		assert.ErrorIs(t, err3, ErrTimeout)
		assert.ErrorIs(t, err4, ErrTimeout)
	})

	t.Run("排他ロック待機中にタイムアウトした場合_待機キューから削除される", func(t *testing.T) {
		// GIVEN
		m := NewManager(50)
		pos := slotPos(0)
		_ = m.Lock(1, pos, Exclusive)

		// WHEN
		err := m.Lock(2, pos, Exclusive)

		// THEN
		assert.ErrorIs(t, err, ErrTimeout)
		m.mutex.Lock()
		state := m.lockTable[pos]
		assert.Equal(t, 0, len(state.waitQueue))
		m.mutex.Unlock()
	})
}

func slotPos(slotNum int) page.SlotPosition {
	return page.SlotPosition{
		PageId:  page.NewPageId(page.FileId(1), page.PageNumber(0)),
		SlotNum: slotNum,
	}
}

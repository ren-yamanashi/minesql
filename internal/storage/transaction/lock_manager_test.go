package transaction

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {
	t.Run("共有ロックを取得できる", func(t *testing.T) {
		// GIVEN
		lm := NewLockManager(1000)
		rId := rowId{pageId: 1, slotNum: 0}

		// WHEN
		err := lm.Lock(1, rId, sharedMode)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("排他ロックを取得できる", func(t *testing.T) {
		// GIVEN
		lm := NewLockManager(1000)
		rId := rowId{pageId: 1, slotNum: 0}

		// WHEN
		err := lm.Lock(1, rId, exclusiveMode)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("複数トランザクションが同じ行に共有ロックを取得できる", func(t *testing.T) {
		// GIVEN
		lm := NewLockManager(1000)
		rId := rowId{pageId: 1, slotNum: 0}

		// WHEN
		err1 := lm.Lock(1, rId, sharedMode)
		err2 := lm.Lock(2, rId, sharedMode)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
	})

	t.Run("既に適切なロックを保持している場合、再取得は何もしない", func(t *testing.T) {
		// GIVEN
		lm := NewLockManager(1000)
		rId := rowId{pageId: 1, slotNum: 0}
		err := lm.Lock(1, rId, exclusiveMode)
		assert.NoError(t, err)

		// WHEN: 排他ロック保持中に共有ロックを要求
		err = lm.Lock(1, rId, sharedMode)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("異なる行には独立してロックを取得できる", func(t *testing.T) {
		// GIVEN
		lm := NewLockManager(1000)
		rId1 := rowId{pageId: 1, slotNum: 0}
		rId2 := rowId{pageId: 1, slotNum: 1}

		// WHEN: 同じトランザクションが別々の行に排他ロック
		err1 := lm.Lock(1, rId1, exclusiveMode)
		err2 := lm.Lock(1, rId2, exclusiveMode)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
	})
}

func TestLockContention(t *testing.T) {
	t.Run("排他ロックが解放されると待機中のトランザクションがロックを取得できる", func(t *testing.T) {
		// GIVEN: trx1 が排他ロックを保持
		lm := NewLockManager(5000)
		rId := rowId{pageId: 1, slotNum: 0}
		err := lm.Lock(1, rId, exclusiveMode)
		assert.NoError(t, err)

		// WHEN: trx2 が共有ロックを要求 (別 goroutine で待機)
		var wg sync.WaitGroup
		var lockErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockErr = lm.Lock(2, rId, sharedMode)
		}()

		// trx1 のロックを解放
		time.Sleep(50 * time.Millisecond)
		lm.UnlockAll(1, []rowId{rId})

		wg.Wait()

		// THEN
		assert.NoError(t, lockErr)
	})

	t.Run("共有ロックが全て解放されると待機中の排他ロックが取得できる", func(t *testing.T) {
		// GIVEN: trx1 と trx2 が共有ロックを保持
		lm := NewLockManager(5000)
		rId := rowId{pageId: 1, slotNum: 0}
		err := lm.Lock(1, rId, sharedMode)
		assert.NoError(t, err)
		err = lm.Lock(2, rId, sharedMode)
		assert.NoError(t, err)

		// WHEN: trx3 が排他ロックを要求 (待機)
		var wg sync.WaitGroup
		var lockErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockErr = lm.Lock(3, rId, exclusiveMode)
		}()

		// trx1 と trx2 のロックを解放
		time.Sleep(50 * time.Millisecond)
		lm.UnlockAll(1, []rowId{rId})
		lm.UnlockAll(2, []rowId{rId})

		wg.Wait()

		// THEN
		assert.NoError(t, lockErr)
	})

	t.Run("タイムアウトするとエラーを返す", func(t *testing.T) {
		// GIVEN: trx1 が排他ロックを保持
		lm := NewLockManager(100) // 100ms タイムアウト
		rId := rowId{pageId: 1, slotNum: 0}
		err := lm.Lock(1, rId, exclusiveMode)
		assert.NoError(t, err)

		// WHEN: trx2 がロックを要求 (trx1 は解放しない)
		err = lm.Lock(2, rId, sharedMode)

		// THEN
		assert.ErrorIs(t, err, ErrLockTimeout)
	})

	t.Run("共有→排他への昇格は他の保持者が解放されると成功する", func(t *testing.T) {
		// GIVEN: trx1 と trx2 が共有ロックを保持
		lm := NewLockManager(5000)
		rId := rowId{pageId: 1, slotNum: 0}
		err := lm.Lock(1, rId, sharedMode)
		assert.NoError(t, err)
		err = lm.Lock(2, rId, sharedMode)
		assert.NoError(t, err)

		// WHEN: trx1 が排他ロックへの昇格を要求 (待機)
		var wg sync.WaitGroup
		var lockErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockErr = lm.Lock(1, rId, exclusiveMode)
		}()

		// trx2 のロックを解放 → trx1 が唯一の保持者になり昇格可能に
		time.Sleep(50 * time.Millisecond)
		lm.UnlockAll(2, []rowId{rId})

		wg.Wait()

		// THEN
		assert.NoError(t, lockErr)
	})
}

func TestUnlockAll(t *testing.T) {
	t.Run("ロックを解放するとエントリがクリーンアップされる", func(t *testing.T) {
		// GIVEN
		lm := NewLockManager(1000)
		rId := rowId{pageId: 1, slotNum: 0}
		err := lm.Lock(1, rId, exclusiveMode)
		assert.NoError(t, err)

		// WHEN
		lm.UnlockAll(1, []rowId{rId})

		// THEN: エントリが削除されている
		lm.mu.Lock()
		_, exists := lm.lockTable[rId]
		lm.mu.Unlock()
		assert.False(t, exists)
	})

	t.Run("複数行のロックを一括解放できる", func(t *testing.T) {
		// GIVEN
		lm := NewLockManager(1000)
		rId1 := rowId{pageId: 1, slotNum: 0}
		rId2 := rowId{pageId: 1, slotNum: 1}
		err := lm.Lock(1, rId1, exclusiveMode)
		assert.NoError(t, err)
		err = lm.Lock(1, rId2, exclusiveMode)
		assert.NoError(t, err)

		// WHEN
		lm.UnlockAll(1, []rowId{rId1, rId2})

		// THEN
		lm.mu.Lock()
		_, exists1 := lm.lockTable[rId1]
		_, exists2 := lm.lockTable[rId2]
		lm.mu.Unlock()
		assert.False(t, exists1)
		assert.False(t, exists2)
	})

	t.Run("存在しない行の解放は無視される", func(t *testing.T) {
		// GIVEN
		lm := NewLockManager(1000)
		rId := rowId{pageId: 99, slotNum: 99}

		// WHEN / THEN: パニックしない
		lm.UnlockAll(1, []rowId{rId})
	})
}

func TestGrantWaitingLocks(t *testing.T) {
	t.Run("排他ロック解放後、待機中の共有ロックが複数付与される", func(t *testing.T) {
		// GIVEN: trx1 が排他ロックを保持、trx2 と trx3 が共有ロックを待機
		lm := NewLockManager(5000)
		rId := rowId{pageId: 1, slotNum: 0}
		err := lm.Lock(1, rId, exclusiveMode)
		assert.NoError(t, err)

		var wg sync.WaitGroup
		var err2, err3 error
		wg.Add(2)
		go func() {
			defer wg.Done()
			err2 = lm.Lock(2, rId, sharedMode)
		}()
		go func() {
			defer wg.Done()
			err3 = lm.Lock(3, rId, sharedMode)
		}()

		// WHEN: trx1 を解放
		time.Sleep(50 * time.Millisecond)
		lm.UnlockAll(1, []rowId{rId})

		wg.Wait()

		// THEN: trx2 と trx3 の両方が共有ロックを取得できる
		assert.NoError(t, err2)
		assert.NoError(t, err3)
	})

	t.Run("排他ロックの待機者がいると後続の共有ロックは付与されない (FIFO)", func(t *testing.T) {
		// GIVEN: trx1 が共有ロックを保持
		lm := NewLockManager(200) // 短めのタイムアウト
		rId := rowId{pageId: 1, slotNum: 0}
		err := lm.Lock(1, rId, sharedMode)
		assert.NoError(t, err)

		// trx2 が排他ロックを待機 (trx1 の共有が邪魔)
		var wg sync.WaitGroup
		wg.Add(2)

		var err2 error
		go func() {
			defer wg.Done()
			err2 = lm.Lock(2, rId, exclusiveMode)
		}()

		// trx3 が共有ロックを要求 (FIFO により trx2 の排他待ちの後ろに並ぶ)
		time.Sleep(20 * time.Millisecond)
		var err3 error
		go func() {
			defer wg.Done()
			err3 = lm.Lock(3, rId, sharedMode)
		}()

		wg.Wait()

		// THEN: trx2 も trx3 もタイムアウト (trx1 が解放しないため)
		assert.ErrorIs(t, err2, ErrLockTimeout)
		assert.ErrorIs(t, err3, ErrLockTimeout)
	})
}

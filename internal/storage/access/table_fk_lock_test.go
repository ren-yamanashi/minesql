package access

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

func TestForeignKeyInsertParentLock(t *testing.T) {
	t.Run("子テーブルへの INSERT は参照先の親レコードに共有ロックを取得する", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		setupTx := env.trxMgr.Begin()
		_ = env.parent.Insert(setupTx, []string{"id", "name"}, []string{"1", "Sales"})
		assert.NoError(t, env.trxMgr.Commit(setupTx))
		// 親 id=1 を別トランザクションが Current Read で排他ロックして保持し続ける
		holder := env.trxMgr.Begin()
		_, found, err := env.parent.SearchForUpdate(holder, SearchModeKey{Key: [][]byte{[]byte("1")}})
		assert.NoError(t, err)
		assert.True(t, found)

		// WHEN
		child := env.trxMgr.Begin()
		err = env.child.Insert(child, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"})

		// THEN
		assert.ErrorIs(t, err, lock.ErrTimeout)
	})

	t.Run("親行の排他ロック解放後に子 INSERT の親共有ロックが付与され INSERT が成功する", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		setupTx := env.trxMgr.Begin()
		_ = env.parent.Insert(setupTx, []string{"id", "name"}, []string{"1", "Sales"})
		assert.NoError(t, env.trxMgr.Commit(setupTx))
		// holder が親 id=1 を Current Read で排他ロック (削除はしない)
		holder := env.trxMgr.Begin()
		_, found, err := env.parent.SearchForUpdate(holder, SearchModeKey{Key: [][]byte{[]byte("1")}})
		assert.NoError(t, err)
		assert.True(t, found)

		// WHEN
		var wg sync.WaitGroup
		var insertErr error
		wg.Go(func() {
			inserter := env.trxMgr.Begin()
			insertErr = env.child.Insert(inserter, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"})
			_ = env.trxMgr.Commit(inserter)
		})
		time.Sleep(10 * time.Millisecond)
		assert.NoError(t, env.trxMgr.Commit(holder))
		wg.Wait()

		// THEN
		assert.NoError(t, insertErr)
	})

	t.Run("親削除中の子 INSERT は親削除コミット後に FK 違反になり孤立子行を防ぐ", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		setupTx := env.trxMgr.Begin()
		_ = env.parent.Insert(setupTx, []string{"id", "name"}, []string{"1", "Sales"})
		assert.NoError(t, env.trxMgr.Commit(setupTx))
		// deleter が親 id=1 を Current Read で排他ロックし論理削除する (未コミットで排他ロック保持)
		deleter := env.trxMgr.Begin()
		target, found, err := env.parent.SearchForUpdate(deleter, SearchModeKey{Key: [][]byte{[]byte("1")}})
		assert.NoError(t, err)
		assert.True(t, found)
		assert.NoError(t, env.parent.SoftDelete(deleter, target))

		// WHEN
		var wg sync.WaitGroup
		var insertErr error
		wg.Go(func() {
			inserter := env.trxMgr.Begin()
			insertErr = env.child.Insert(inserter, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"})
		})
		time.Sleep(10 * time.Millisecond)
		assert.NoError(t, env.trxMgr.Commit(deleter))
		wg.Wait()

		// THEN
		assert.ErrorIs(t, insertErr, ErrForeignKeyViolation)
	})
}

func TestForeignKeyParentDeleteChildLock(t *testing.T) {
	t.Run("親 DELETE は子側の未コミット SoftDelete に対する子側 X-Lock 解放まで待つ", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		setupTx := env.trxMgr.Begin()
		_ = env.parent.Insert(setupTx, []string{"id", "name"}, []string{"1", "Sales"})
		_ = env.child.Insert(setupTx, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"})
		assert.NoError(t, env.trxMgr.Commit(setupTx))

		// 子側で child id=1 を Current Read で排他ロックし論理削除する (未コミットで子側 SK+PK の X-Lock 保持)
		childDeleter := env.trxMgr.Begin()
		childRec, found, err := env.child.SearchForUpdate(childDeleter, SearchModeKey{Key: [][]byte{[]byte("1")}})
		assert.NoError(t, err)
		assert.True(t, found)
		assert.NoError(t, env.child.SoftDelete(childDeleter, childRec))

		// WHEN
		parentDeleter := env.trxMgr.Begin()
		parentRec, found, err := env.parent.SearchForUpdate(parentDeleter, SearchModeKey{Key: [][]byte{[]byte("1")}})
		assert.NoError(t, err)
		assert.True(t, found)
		// 親 SoftDelete の FK 検査が子側セカンダリレコードに S-Lock を取りに行くが、子側 X-Lock があるため待機 → タイムアウト
		err = env.parent.SoftDelete(parentDeleter, parentRec)

		// THEN
		assert.ErrorIs(t, err, lock.ErrTimeout)
	})

	t.Run("子 SoftDelete の Rollback 後に親 DELETE が active 復活した子行を検出して FK 違反になる", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		setupTx := env.trxMgr.Begin()
		_ = env.parent.Insert(setupTx, []string{"id", "name"}, []string{"1", "Sales"})
		_ = env.child.Insert(setupTx, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"})
		assert.NoError(t, env.trxMgr.Commit(setupTx))

		childDeleter := env.trxMgr.Begin()
		childRec, found, err := env.child.SearchForUpdate(childDeleter, SearchModeKey{Key: [][]byte{[]byte("1")}})
		assert.NoError(t, err)
		assert.True(t, found)
		assert.NoError(t, env.child.SoftDelete(childDeleter, childRec))

		// WHEN
		var wg sync.WaitGroup
		var parentDeleteErr error
		wg.Go(func() {
			parentDeleter := env.trxMgr.Begin()
			parentRec, _, err := env.parent.SearchForUpdate(parentDeleter, SearchModeKey{Key: [][]byte{[]byte("1")}})
			assert.NoError(t, err)
			parentDeleteErr = env.parent.SoftDelete(parentDeleter, parentRec)
		})
		time.Sleep(10 * time.Millisecond)
		// 子側 Rollback で子行が active に戻る
		assert.NoError(t, env.trxMgr.Rollback(childDeleter))
		wg.Wait()

		// THEN
		assert.ErrorIs(t, parentDeleteErr, ErrForeignKeyViolation)
	})

	t.Run("子 SoftDelete の Commit 後に親 DELETE が成功する", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		setupTx := env.trxMgr.Begin()
		_ = env.parent.Insert(setupTx, []string{"id", "name"}, []string{"1", "Sales"})
		_ = env.child.Insert(setupTx, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"})
		assert.NoError(t, env.trxMgr.Commit(setupTx))

		childDeleter := env.trxMgr.Begin()
		childRec, found, err := env.child.SearchForUpdate(childDeleter, SearchModeKey{Key: [][]byte{[]byte("1")}})
		assert.NoError(t, err)
		assert.True(t, found)
		assert.NoError(t, env.child.SoftDelete(childDeleter, childRec))

		// WHEN
		var wg sync.WaitGroup
		var parentDeleteErr error
		wg.Go(func() {
			parentDeleter := env.trxMgr.Begin()
			parentRec, _, err := env.parent.SearchForUpdate(parentDeleter, SearchModeKey{Key: [][]byte{[]byte("1")}})
			assert.NoError(t, err)
			parentDeleteErr = env.parent.SoftDelete(parentDeleter, parentRec)
		})
		time.Sleep(10 * time.Millisecond)
		// 子側 Commit で子行は論理削除済み (deleteMark=1) になる
		assert.NoError(t, env.trxMgr.Commit(childDeleter))
		wg.Wait()

		// THEN
		assert.NoError(t, parentDeleteErr)
	})
}

func TestConcurrentFKCheckNoDeadlock(t *testing.T) {
	t.Run("子 SoftDelete の Rollback と並行する親 SoftDelete が反復実行で deadlock しない", func(t *testing.T) {
		const iterations = 10
		for range iterations {
			// GIVEN
			_ = os.RemoveAll(config.BaseDir)
			env := setupFKTestEnv(t)
			setupTx := env.trxMgr.Begin()
			_ = env.parent.Insert(setupTx, []string{"id", "name"}, []string{"1", "Sales"})
			_ = env.child.Insert(setupTx, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"})
			assert.NoError(t, env.trxMgr.Commit(setupTx))

			childDeleter := env.trxMgr.Begin()
			childRec, found, err := env.child.SearchForUpdate(childDeleter, SearchModeKey{Key: [][]byte{[]byte("1")}})
			assert.NoError(t, err)
			assert.True(t, found)
			assert.NoError(t, env.child.SoftDelete(childDeleter, childRec))

			// WHEN
			var wg sync.WaitGroup
			var parentDeleteErr error
			wg.Go(func() {
				parentDeleter := env.trxMgr.Begin()
				parentRec, _, err := env.parent.SearchForUpdate(parentDeleter, SearchModeKey{Key: [][]byte{[]byte("1")}})
				assert.NoError(t, err)
				parentDeleteErr = env.parent.SoftDelete(parentDeleter, parentRec)
			})
			time.Sleep(10 * time.Millisecond)
			assert.NoError(t, env.trxMgr.Rollback(childDeleter))
			wg.Wait()

			// THEN
			assert.ErrorIs(t, parentDeleteErr, ErrForeignKeyViolation)
		}
	})
}

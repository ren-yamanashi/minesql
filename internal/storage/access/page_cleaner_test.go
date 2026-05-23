package access

import (
	"testing"
	"time"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewPageCleaner(t *testing.T) {
	t.Run("PageCleaner を生成できる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)

		// WHEN
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 90)

		// THEN
		assert.NotNil(t, pc)
		assert.Equal(t, 1024*1024, pc.redoLogMaxSize)
		assert.Equal(t, 90, pc.maxDirtyPagePct)
	})
}

func TestPageCleanerStartStop(t *testing.T) {
	t.Run("Start と Stop が正常に動作する", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 90)

		// WHEN
		pc.Start()
		time.Sleep(10 * time.Millisecond)
		pc.Stop()

		// THEN
		assert.False(t, pc.isRunning)
	})

	t.Run("Stop を二重呼び出ししてもパニックしない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 90)
		pc.Start()
		time.Sleep(10 * time.Millisecond)

		// WHEN / THEN
		assert.NotPanics(t, func() {
			pc.Stop()
			pc.Stop()
		})
	})

	t.Run("Start せずに Stop してもパニックしない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 90)

		// WHEN / THEN
		assert.NotPanics(t, func() {
			pc.Stop()
		})
	})

	t.Run("Start を二重呼び出しすると最初の goroutine が維持される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 90)
		pc.Start()

		// WHEN
		pc.Start() // 二重呼び出し

		// THEN
		assert.True(t, pc.isRunning)
		pc.Stop()
		assert.False(t, pc.isRunning)
	})

	t.Run("Stop 後に再度 Start できる", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 90)
		pc.Start()
		pc.Stop()

		// WHEN
		pc.Start()
		time.Sleep(10 * time.Millisecond)
		pc.Stop()

		// THEN
		assert.False(t, pc.isRunning)
	})
}

func TestPageCleanerClean(t *testing.T) {
	t.Run("ダーティーページがない場合フラッシュしない", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 90)

		// WHEN
		err := pc.clean()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("Redo ログサイズが閾値を超えるとフラッシュが実行される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		// ダーティーページを作成
		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		writePage, _ := env.bp.PageForWrite(pgId)
		writePage.Page.Body[0] = 0xAA

		// Redo ログに大量のレコードを追加して閾値 (100 バイト) を超えさせる
		pc := NewPageCleaner(env.bp, env.redoLog, 100, 90)
		for range 10 {
			pg := buildRedoTestPage(t)
			env.redoLog.AppendPageCopy(lock.TrxId(1), pgId, *pg)
		}
		_ = env.redoLog.Flush()

		flushListSizeBefore := env.bp.NumOfFlushListPage()

		// WHEN
		err := pc.clean()

		// THEN
		assert.NoError(t, err)
		assert.Less(t, env.bp.NumOfFlushListPage(), flushListSizeBefore)
	})

	t.Run("ダーティーページ率が閾値を超えるとフラッシュが実行される", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		// ダーティーページを作成
		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		_, _ = env.bp.PageForWrite(pgId)

		// maxDirtyPct=0 にすると 1 ページでも閾値超過
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 0)

		// WHEN
		err := pc.clean()

		// THEN
		assert.NoError(t, err)
	})
}

func TestPageCleanerShouldFlush(t *testing.T) {
	t.Run("ダーティーページがない場合 false を返す", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 90)

		// WHEN
		result, err := pc.shouldFlush()

		// THEN
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Redo ログサイズが閾値以下かつダーティーページ率も閾値以下なら false を返す", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		_, _ = env.bp.PageForWrite(pgId)

		// 閾値を大きくして超えないようにする
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 90)

		// WHEN
		result, err := pc.shouldFlush()

		// THEN
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Redo ログサイズが閾値を超えると true を返す", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		_, _ = env.bp.PageForWrite(pgId)

		// 閾値を極小に設定
		pc := NewPageCleaner(env.bp, env.redoLog, 1, 90)

		// WHEN
		result, err := pc.shouldFlush()

		// THEN
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("ダーティーページ率が閾値を超えると true を返す", func(t *testing.T) {
		// GIVEN
		env := setupRecoveryTestEnv(t)
		_ = env.bp.FlushAllPages()

		pgId := page.NewId(page.FileId(2), 0)
		_, _ = env.bp.AddPage(pgId)
		_, _ = env.bp.PageForWrite(pgId)

		// ダーティーページ率の閾値を 0 に設定
		pc := NewPageCleaner(env.bp, env.redoLog, 1024*1024, 0)

		// WHEN
		result, err := pc.shouldFlush()

		// THEN
		assert.NoError(t, err)
		assert.True(t, result)
	})
}

// buildRedoTestPage はテスト用の page.Page を作成する
func buildRedoTestPage(t *testing.T) *page.Page {
	t.Helper()
	data := make([]byte, page.PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	pg, err := page.NewPage(data)
	if err != nil {
		t.Fatalf("テストページの作成に失敗: %v", err)
	}
	return pg
}

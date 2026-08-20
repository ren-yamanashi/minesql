package btree

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFreeStep(t *testing.T) {
	t.Run("高さ 1 の最小の木を FreeStep で解放できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		tree, err := createTreeForTest(t, bp, page.FileId(0))
		require.NoError(t, err)
		pageIds := collectAllPageIdsInIsolatedMtr(t, bp, tree)

		// WHEN
		runFreeStepUntilDone(t, bp, tree)

		// THEN
		assertPageIdsAllFree(t, bp, pageIds)
	})

	t.Run("複数階層の木を FreeStep で解放すると事前列挙した全ページが free になる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		tree, err := createTreeForTest(t, bp, page.FileId(0))
		require.NoError(t, err)
		insertRecordsForTree(t, tree, 30, 1500)
		pageIds := collectAllPageIdsInIsolatedMtr(t, bp, tree)
		require.Greater(t, len(pageIds), 2)

		// WHEN
		runFreeStepUntilDone(t, bp, tree)

		// THEN
		assertPageIdsAllFree(t, bp, pageIds)
	})

	t.Run("解放完了後の再呼び出しは true を返し状態が変わらない", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeBufferPool(t)
		tree, err := createTreeForTest(t, bp, page.FileId(0))
		require.NoError(t, err)
		insertRecordsForTree(t, tree, 10, 1500)
		pageIds := collectAllPageIdsInIsolatedMtr(t, bp, tree)
		runFreeStepUntilDone(t, bp, tree)
		assertPageIdsAllFree(t, bp, pageIds)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		done, err := tree.FreeStep(mtr)
		require.NoError(t, err)
		require.NoError(t, mtr.Commit())

		// THEN
		assert.True(t, done)
		assertPageIdsAllFree(t, bp, pageIds)
	})

	t.Run("extent を持つ規模の木は 1 呼び出しで完了せず複数 step を要する", func(t *testing.T) {
		// GIVEN: 128 frag slot を使い切り extent 獲得が必ず発生する規模の木
		bp := newLargeFreeStepBufferPool(t)
		tree, err := createTreeForTest(t, bp, page.FileId(0))
		require.NoError(t, err)
		insertRecordsForTree(t, tree, 300, 1500)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		done, err := tree.FreeStep(mtr)
		require.NoError(t, err)
		require.NoError(t, mtr.Commit())

		// THEN
		assert.False(t, done)
	})
}

// collectAllPageIdsInIsolatedMtr は独立の書き込み mtr で collectAllPageIds を呼ぶ
func collectAllPageIdsInIsolatedMtr(t *testing.T, bp *buffer.Pool, tree *Tree) []page.Id {
	t.Helper()
	mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
	defer mtr.UnpinAll()
	return collectAllPageIds(t, tree, mtr)
}

// runFreeStepUntilDone は FreeStep を done まで独立 mtr で繰り返す
func runFreeStepUntilDone(t *testing.T, bp *buffer.Pool, tree *Tree) {
	t.Helper()
	const maxSteps = 10000
	for step := range maxSteps {
		mtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, newTestRedoBuffer(t))
		done, err := tree.FreeStep(mtr)
		require.NoError(t, err)
		require.NoError(t, mtr.Commit())
		if done {
			return
		}
		if step == maxSteps-1 {
			t.Fatalf("FreeStep が %d step 経っても完了しない", maxSteps)
		}
	}
}

// assertPageIdsAllFree は与えられた PageId 群が fsp 的に全て free であることを検証する
func assertPageIdsAllFree(t *testing.T, bp *buffer.Pool, pageIds []page.Id) {
	t.Helper()
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	for _, pid := range pageIds {
		isFree, err := fsp.IsPageFree(mtr, pid)
		require.NoError(t, err)
		assert.True(t, isFree, "page %v が解放されていない", pid)
	}
}

// insertRecordsForTree はキーを連番でインクリメントして count 件挿入する
func insertRecordsForTree(t *testing.T, tree *Tree, count int, valueSize int) {
	t.Helper()
	value := make([]byte, valueSize)
	for i := range count {
		key := []byte{byte(i / 256), byte(i % 256)}
		mtr := buffer.NewMtr(tree.bufferPool)
		require.NoError(t, tree.Insert(mtr, NewRecord([]byte{}, key, value)))
		mtr.UnpinAll()
	}
}

// newLargeFreeStepBufferPool は大きい木の生成・解放に耐える広めのバッファプールを作る
func newLargeFreeStepBufferPool(t *testing.T) *buffer.Pool {
	t.Helper()
	bp := newTestBufferPool(t, page.Size*300)
	path := filepath.Join(t.TempDir(), "free_step_large.db")
	hf, err := file.NewHeapFile(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(page.FileId(0), hf)
	return bp
}

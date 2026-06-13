package btree

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewTree(t *testing.T) {
	t.Run("既存の B+Tree を開ける", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		created, _ := CreateTree(bp, page.FileId(0))

		// WHEN
		bt := NewTree(bp, created.MetaPageId())

		// THEN
		assert.Equal(t, created.MetaPageId(), bt.MetaPageId())
	})

	t.Run("NewTree で開いた B+Tree のメタデータを読み取れる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		created, _ := CreateTree(bp, page.FileId(0))

		// WHEN
		bt := NewTree(bp, created.MetaPageId())
		count, err := bt.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})

	t.Run("NewTree でツリーラッチが初期化される", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		created, _ := CreateTree(bp, page.FileId(0))

		// WHEN
		bt := NewTree(bp, created.MetaPageId())

		// THEN
		assert.NotNil(t, bt.latch)
	})
}

func TestCreateTree(t *testing.T) {
	t.Run("B+Tree を作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)

		// WHEN
		bt, err := CreateTree(bp, page.FileId(0))

		// THEN
		assert.NoError(t, err)
		assert.False(t, bt.MetaPageId().IsInvalid())
	})

	t.Run("作成後のリーフページ数は 1 になる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)

		// WHEN
		bt, err := CreateTree(bp, page.FileId(0))
		assert.NoError(t, err)
		count, err := bt.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})

	t.Run("作成後の高さは 1 になる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)

		// WHEN
		bt, err := CreateTree(bp, page.FileId(0))
		assert.NoError(t, err)
		height, err := bt.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})

	t.Run("CreateTree でツリーラッチが初期化される", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)

		// WHEN
		bt, err := CreateTree(bp, page.FileId(0))

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, bt.latch)
	})
}

func TestTreeLatchUsableViaMtr(t *testing.T) {
	t.Run("ツリーラッチを Mtr 経由で取得・解放できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bp)

		// WHEN
		mtr.LockShared(bt.latch)

		// THEN
		assert.Equal(t, 1, mtr.HeldLatchCount())
		mtr.UnpinAll()
		assert.Equal(t, 0, mtr.HeldLatchCount())
	})
}

func TestOptimisticHeight1(t *testing.T) {
	t.Run("高さ 1 (ルート = リーフ) の木で楽観挿入・更新・削除ができる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))
		mtr := buffer.NewMtr(bt.bufferPool)
		defer mtr.UnpinAll()
		heightBefore, _ := bt.Height()
		assert.Equal(t, uint64(1), heightBefore)

		// WHEN: 楽観挿入
		assert.NoError(t, bt.Insert(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xAA})))
		assert.NoError(t, bt.Insert(mtr, NewRecord([]byte{}, []byte{0x20}, []byte{0xBB})))

		// THEN: 挿入したレコードを検索できる
		rec, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xAA}, rec.NonKey())

		// WHEN: 楽観更新
		assert.NoError(t, bt.Update(mtr, NewRecord([]byte{}, []byte{0x10}, []byte{0xCC})))

		// THEN: 更新後の値で検索できる
		updated, _, err := bt.FindByKey(mtr, []byte{0x10})
		assert.NoError(t, err)
		assert.Equal(t, []byte{0xCC}, updated.NonKey())

		// WHEN: 楽観削除
		assert.NoError(t, bt.Delete(mtr, []byte{0x20}))

		// THEN: 削除したキーは見つからず、高さは 1 のまま
		_, _, err = bt.FindByKey(mtr, []byte{0x20})
		assert.ErrorIs(t, err, ErrKeyNotFound)
		heightAfter, _ := bt.Height()
		assert.Equal(t, uint64(1), heightAfter)
	})
}

func TestLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))

		// WHEN
		count, err := bt.LeafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestHeight(t *testing.T) {
	t.Run("B+Tree の高さを取得できる", func(t *testing.T) {
		// GIVEN
		bp := setupBtreeTestBufferPool(t)
		bt, _ := CreateTree(bp, page.FileId(0))

		// WHEN
		height, err := bt.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// setupBtreeTestBufferPool はテスト用のバッファプールを作成する
func setupBtreeTestBufferPool(t *testing.T) *buffer.Pool {
	t.Helper()
	bp := newTestBufferPool(page.Size * 10)
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(0, path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(0, hf)
	return bp
}

package buffer

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestHasHeapFile(t *testing.T) {
	t.Run("登録されている FileId に対しては true を返す", func(t *testing.T) {
		// GIVEN
		bp, _ := setupDeallocTestEnv(t)
		fileId := page.FileId(5)
		registerHeapFile(t, bp, fileId)

		// WHEN
		ok := bp.HasHeapFile(fileId)

		// THEN
		assert.True(t, ok)
	})

	t.Run("登録されていない FileId に対しては false を返す", func(t *testing.T) {
		// GIVEN
		bp, _ := setupDeallocTestEnv(t)

		// WHEN
		ok := bp.HasHeapFile(page.FileId(99))

		// THEN
		assert.False(t, ok)
	})

	t.Run("DeleteFile 後の FileId に対しては false を返す", func(t *testing.T) {
		// GIVEN
		bp, _ := setupDeallocTestEnv(t)
		fileId := page.FileId(5)
		registerHeapFile(t, bp, fileId)
		assert.NoError(t, bp.DeleteFile(fileId))

		// WHEN
		ok := bp.HasHeapFile(fileId)

		// THEN
		assert.False(t, ok)
	})
}

func TestIsPageInFileFreeList(t *testing.T) {
	t.Run("解放されていないページに対しては false を返す", func(t *testing.T) {
		// GIVEN
		bp, mapPageId := setupDeallocTestEnv(t)
		fileId := page.FileId(2)
		registerHeapFile(t, bp, fileId)
		notFreed := allocatePageInFile(t, bp, fileId)

		// WHEN
		mtr := NewMtr(bp)
		defer mtr.UnpinAll()
		inList, err := bp.IsPageInFileFreeList(mtr, mapPageId, notFreed)

		// THEN
		assert.NoError(t, err)
		assert.False(t, inList)
	})

	t.Run("Deallocate 直後のページに対しては true を返す", func(t *testing.T) {
		// GIVEN
		bp, mapPageId := setupDeallocTestEnv(t)
		fileId := page.FileId(2)
		registerHeapFile(t, bp, fileId)
		freed := allocatePageInFile(t, bp, fileId)
		writeMtr := NewWriteMtr(bp, lock.SystemReservedTrxId, newDeallocTestRedoBuffer(t))
		assert.NoError(t, bp.Deallocate(writeMtr, mapPageId, freed))
		assert.NoError(t, writeMtr.Commit())

		// WHEN
		mtr := NewMtr(bp)
		defer mtr.UnpinAll()
		inList, err := bp.IsPageInFileFreeList(mtr, mapPageId, freed)

		// THEN
		assert.NoError(t, err)
		assert.True(t, inList)
	})

	t.Run("リンクリスト中間のページに対しても true を返す", func(t *testing.T) {
		// GIVEN
		bp, mapPageId := setupDeallocTestEnv(t)
		fileId := page.FileId(2)
		registerHeapFile(t, bp, fileId)
		first := allocatePageInFile(t, bp, fileId)
		second := allocatePageInFile(t, bp, fileId)
		writeMtr := NewWriteMtr(bp, lock.SystemReservedTrxId, newDeallocTestRedoBuffer(t))
		assert.NoError(t, bp.Deallocate(writeMtr, mapPageId, first))
		assert.NoError(t, bp.Deallocate(writeMtr, mapPageId, second))
		assert.NoError(t, writeMtr.Commit())

		// WHEN
		mtr := NewMtr(bp)
		defer mtr.UnpinAll()
		inList, err := bp.IsPageInFileFreeList(mtr, mapPageId, first)

		// THEN
		assert.NoError(t, err)
		assert.True(t, inList)
	})

	t.Run("該当 FileId にフリーリストが存在しない場合は false を返す", func(t *testing.T) {
		// GIVEN
		bp, mapPageId := setupDeallocTestEnv(t)
		fileId := page.FileId(2)
		registerHeapFile(t, bp, fileId)
		target := allocatePageInFile(t, bp, fileId)

		// WHEN
		mtr := NewMtr(bp)
		defer mtr.UnpinAll()
		inList, err := bp.IsPageInFileFreeList(mtr, mapPageId, target)

		// THEN
		assert.NoError(t, err)
		assert.False(t, inList)
	})
}

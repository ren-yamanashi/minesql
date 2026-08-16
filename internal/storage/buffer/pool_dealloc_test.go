package buffer

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

func TestDeallocate(t *testing.T) {
	t.Run("解放したページがマップページのフリーリスト先頭になる", func(t *testing.T) {
		// GIVEN
		bp, mapPageId := setupDeallocTestEnv(t)
		targetFileId := page.FileId(2)
		registerHeapFile(t, bp, targetFileId)
		freed := allocatePageInFile(t, bp, targetFileId, 0)

		// WHEN
		mtr := NewWriteMtr(bp, lock.SystemReservedTrxId, newDeallocTestRedoBuffer(t))
		err := bp.Deallocate(mtr, mapPageId, freed)
		assert.NoError(t, err)
		assert.NoError(t, mtr.Commit())

		// THEN
		head := readHeadPageNumber(t, bp, mapPageId, targetFileId)
		assert.Equal(t, freed.PageNumber(), head)
	})

	t.Run("複数回の解放が単方向リンクリストを形成する", func(t *testing.T) {
		// GIVEN
		bp, mapPageId := setupDeallocTestEnv(t)
		targetFileId := page.FileId(2)
		registerHeapFile(t, bp, targetFileId)
		first := allocatePageInFile(t, bp, targetFileId, 0)
		second := allocatePageInFile(t, bp, targetFileId, 1)

		// WHEN
		mtr1 := NewWriteMtr(bp, lock.SystemReservedTrxId, newDeallocTestRedoBuffer(t))
		assert.NoError(t, bp.Deallocate(mtr1, mapPageId, first))
		assert.NoError(t, mtr1.Commit())
		mtr2 := NewWriteMtr(bp, lock.SystemReservedTrxId, newDeallocTestRedoBuffer(t))
		assert.NoError(t, bp.Deallocate(mtr2, mapPageId, second))
		assert.NoError(t, mtr2.Commit())

		// THEN
		head := readHeadPageNumber(t, bp, mapPageId, targetFileId)
		assert.Equal(t, second.PageNumber(), head)
		next := readNextPointer(t, bp, second)
		assert.Equal(t, first.PageNumber(), next)
		end := readNextPointer(t, bp, first)
		assert.Equal(t, page.MaxPageNumber, end)
	})

	t.Run("複数の FileId は独立に解放できる", func(t *testing.T) {
		// GIVEN
		bp, mapPageId := setupDeallocTestEnv(t)
		fileIdA := page.FileId(2)
		fileIdB := page.FileId(3)
		registerHeapFile(t, bp, fileIdA)
		registerHeapFile(t, bp, fileIdB)
		freedA := allocatePageInFile(t, bp, fileIdA, 0)
		freedB := allocatePageInFile(t, bp, fileIdB, 0)

		// WHEN
		mtr := NewWriteMtr(bp, lock.SystemReservedTrxId, newDeallocTestRedoBuffer(t))
		assert.NoError(t, bp.Deallocate(mtr, mapPageId, freedA))
		assert.NoError(t, bp.Deallocate(mtr, mapPageId, freedB))
		assert.NoError(t, mtr.Commit())

		// THEN
		assert.Equal(t, freedA.PageNumber(), readHeadPageNumber(t, bp, mapPageId, fileIdA))
		assert.Equal(t, freedB.PageNumber(), readHeadPageNumber(t, bp, mapPageId, fileIdB))
	})
}

func setupDeallocTestEnv(t *testing.T) (*Pool, page.Id) {
	t.Helper()
	bp, _ := setupFreeListMapTestPool(t)
	mapPageId := page.NewId(page.FileId(0), 0)
	_, err := bp.AddPage(mapPageId)
	assert.NoError(t, err)
	mtr := NewWriteMtr(bp, lock.SystemReservedTrxId, newDeallocTestRedoBuffer(t))
	bufPage, err := mtr.PageForWrite(mapPageId)
	assert.NoError(t, err)
	InitializeFreeListMapPage(bufPage)
	assert.NoError(t, mtr.Commit())
	return bp, mapPageId
}

func newDeallocTestRedoBuffer(t *testing.T) *redo.Buffer {
	t.Helper()
	rl, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = rl.Close() })
	return rl
}

func registerHeapFile(t *testing.T, bp *Pool, fileId page.FileId) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dealloc_test.db")
	hf, err := file.NewHeapFile(path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	bp.RegisterHeapFile(fileId, hf)
}

func allocatePageInFile(t *testing.T, bp *Pool, fileId page.FileId, pageNumber page.PageNumber) page.Id {
	t.Helper()
	pageId := page.NewId(fileId, pageNumber)
	_, err := bp.AddPage(pageId)
	assert.NoError(t, err)
	return pageId
}

func readHeadPageNumber(t *testing.T, bp *Pool, mapPageId page.Id, fileId page.FileId) page.PageNumber {
	t.Helper()
	bufPage, err := bp.Page(mapPageId)
	assert.NoError(t, err)
	defer bp.Unpin(mapPageId)
	return newFreeListMapPage(bufPage).headPageNumber(fileId)
}

func readNextPointer(t *testing.T, bp *Pool, pageId page.Id) page.PageNumber {
	t.Helper()
	bufPage, err := bp.Page(pageId)
	assert.NoError(t, err)
	defer bp.Unpin(pageId)
	return page.PageNumber(binary.BigEndian.Uint32(bufPage.Data().Body()[0:4]))
}

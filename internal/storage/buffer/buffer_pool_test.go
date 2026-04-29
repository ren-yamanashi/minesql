package buffer

import (
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewBufferPool(t *testing.T) {
	t.Run("サイズが PageSize 以下の場合 MaxNumOfPage が 1 になる", func(t *testing.T) {
		// GIVEN / WHEN
		bp := NewBufferPool(page.PageSize)

		// THEN
		assert.Equal(t, uint32(1), bp.MaxNumOfPage)
	})

	t.Run("サイズが PageSize より大きい場合 MaxNumOfPage が算出される", func(t *testing.T) {
		// GIVEN / WHEN
		bp := NewBufferPool(page.PageSize * 3)

		// THEN
		assert.Equal(t, uint32(4), bp.MaxNumOfPage) // 3 + 1
	})

	t.Run("サイズが 0 の場合 MaxNumOfPage が 1 になる", func(t *testing.T) {
		// GIVEN / WHEN
		bp := NewBufferPool(0)

		// THEN
		assert.Equal(t, uint32(1), bp.MaxNumOfPage)
	})
}

func TestAllocatePageId(t *testing.T) {
	t.Run("新しい PageId を割り当てられる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize)
		hf := setupHeapFile(t, 5)
		bp.RegisterHeapFile(5, hf)

		// WHEN
		id, err := bp.AllocatePageId(5)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.FileId(5), id.FileId)
		assert.Equal(t, page.PageNumber(0), id.PageNumber)
	})

	t.Run("連続で割り当てると PageNumber がインクリメントされる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)

		// WHEN
		id1, err := bp.AllocatePageId(0)
		assert.NoError(t, err)
		id2, err := bp.AllocatePageId(0)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, page.PageNumber(0), id1.PageNumber)
		assert.Equal(t, page.PageNumber(1), id2.PageNumber)
	})

	t.Run("未登録の FileId の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize)

		// WHEN
		id, err := bp.AllocatePageId(99)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, page.InvalidPageId, id)
	})
}

func TestRegisterHeapFile(t *testing.T) {
	t.Run("HeapFile を登録すると取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize)
		hf := setupHeapFile(t, 1)

		// WHEN
		bp.RegisterHeapFile(1, hf)

		// THEN
		got, err := bp.GetHeapFile(1)
		assert.NoError(t, err)
		assert.Equal(t, hf, got)
	})
}

func TestGetHeapFile(t *testing.T) {
	t.Run("登録済みの HeapFile を取得できる", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize)
		hf := setupHeapFile(t, 0)
		bp.RegisterHeapFile(0, hf)

		// WHEN
		got, err := bp.GetHeapFile(0)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, hf, got)
	})

	t.Run("未登録の FileId の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		bp := NewBufferPool(page.PageSize)

		// WHEN
		got, err := bp.GetHeapFile(99)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, got)
	})
}

func setupHeapFile(t *testing.T, fileId page.FileId) *file.HeapFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(fileId, path)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })
	return hf
}

func writePageToDisk(t *testing.T, hf *file.HeapFile, pageNum page.PageNumber, value byte) {
	t.Helper()
	data := directio.AlignedBlock(page.PageSize)
	data[page.PageHeaderSize] = value
	err := hf.Write(pageNum, data)
	assert.NoError(t, err)
}

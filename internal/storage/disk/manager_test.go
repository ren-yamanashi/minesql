package disk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDiskManager(t *testing.T) {
	t.Run("正常に DiskManager が生成される", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_disk.db")
		assert.NoError(t, err)
		defer tmpFile.Close()
		defer os.Remove(tmpFile.Name())

		// GIVEN
		filepath := tmpFile.Name()
		fileId := FileId(0)

		// WHEN
		disk, err := NewDiskManager(fileId, filepath)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, NewPageId(fileId, PageNumber(0)), disk.nextPageId)
	})

	t.Run("無効なファイルが指定された場合はエラー", func(t *testing.T) {
		// GIVEN
		invalidPath := "/nonexistent/directory/file.db"
		fileId := FileId(0)

		// WHEN
		_, err := NewDiskManager(fileId, invalidPath)

		// THEN
		assert.Error(t, err)
	})
}

func TestReadPageData(t *testing.T) {
	t.Run("正常にデータを読み込める", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDiskManager(t)
		writeData := createDataBuffer()
		disk.WritePageData(pageId, writeData)

		// WHEN
		readData := make([]byte, PAGE_SIZE)
		err := disk.ReadPageData(pageId, readData)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, writeData, readData)
	})

	t.Run("書き込むデータのサイズが PAGE_SIZE と異なる場合はエラー", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDiskManager(t)
		invalidData := make([]byte, PAGE_SIZE-1)

		// WHEN
		err := disk.ReadPageData(pageId, invalidData)

		// THEN
		assert.Error(t, err)
	})
}

func TestWritePageData(t *testing.T) {
	t.Run("正常にデータを書き込める", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDiskManager(t)
		writeData := createDataBuffer()

		// WHEN
		err := disk.WritePageData(pageId, writeData)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, NewPageId(FileId(0), PageNumber(1)), disk.nextPageId)
	})

	t.Run("書き込むデータのサイズが PAGE_SIZE と異なる場合はエラー", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDiskManager(t)
		invalidData := make([]byte, PAGE_SIZE+10)

		// WHEN
		err := disk.WritePageData(pageId, invalidData)

		// THEN
		assert.Error(t, err)
	})
}

func TestAllocatePage(t *testing.T) {
	t.Run("新しいページを順次割り当てられる", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_disk_*.db")
		assert.NoError(t, err)
		defer tmpFile.Close()
		defer os.Remove(tmpFile.Name())

		// GIVEN
		filepath := tmpFile.Name()
		fileId := FileId(0)

		dm, err := NewDiskManager(fileId, filepath)
		assert.NoError(t, err)

		// WHEN
		pageId1 := dm.AllocatePage()
		pageId2 := dm.AllocatePage()
		pageId3 := dm.AllocatePage()

		// THEN
		assert.Equal(t, NewPageId(fileId, PageNumber(0)), pageId1)
		assert.Equal(t, NewPageId(fileId, PageNumber(1)), pageId2)
		assert.Equal(t, NewPageId(fileId, PageNumber(2)), pageId3)
		assert.Equal(t, NewPageId(fileId, PageNumber(3)), dm.nextPageId)
	})
}

func initDiskManager(t *testing.T) (*DiskManager, PageId) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "sample.db")
	dm, err := NewDiskManager(FileId(0), dbPath)
	if err != nil {
		t.Fatalf("Failed to open DiskManager: %v", err)
	}
	pageId := dm.AllocatePage()
	return dm, pageId
}

func createDataBuffer() []byte {
	writeData := make([]byte, PAGE_SIZE)
	for i := range PAGE_SIZE {
		writeData[i] = byte(i % 256)
	}
	return writeData
}

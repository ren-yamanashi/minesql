package file

import (
	"path/filepath"
	"testing"

	"minesql/internal/storage/page"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

func TestNewDisk(t *testing.T) {
	t.Run("正常に Disk が生成される", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		fileId := page.FileId(0)

		// WHEN
		disk, err := NewDisk(fileId, dbPath)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(fileId, page.PageNumber(0)), disk.nextPageId)
	})

	t.Run("無効なファイルが指定された場合はエラー", func(t *testing.T) {
		// GIVEN
		invalidPath := "/nonexistent/directory/file.db"
		fileId := page.FileId(0)

		// WHEN
		_, err := NewDisk(fileId, invalidPath)

		// THEN
		assert.Error(t, err)
	})

	t.Run("既存のデータファイルを開いた場合、nextPageId がファイルサイズに基づいて設定される", func(t *testing.T) {
		// GIVEN
		// 1 ページ分のデータを書き込んだファイルを用意
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		fileId := page.FileId(0)

		dm1, err := NewDisk(fileId, dbPath)
		assert.NoError(t, err)

		pageId := dm1.AllocatePage()
		writeData := createDataBuffer()
		err = dm1.WritePageData(pageId, writeData)
		assert.NoError(t, err)

		// WHEN
		dm2, err := NewDisk(fileId, dbPath)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, page.NewPageId(fileId, page.PageNumber(1)), dm2.nextPageId)
	})
}

func TestAllocatePage(t *testing.T) {
	t.Run("新しいページを順次割り当てられる", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		fileId := page.FileId(0)
		disk, err := NewDisk(fileId, dbPath)
		assert.NoError(t, err)

		// WHEN
		pageId1 := disk.AllocatePage()
		pageId2 := disk.AllocatePage()
		pageId3 := disk.AllocatePage()

		// THEN
		assert.Equal(t, page.NewPageId(fileId, page.PageNumber(0)), pageId1)
		assert.Equal(t, page.NewPageId(fileId, page.PageNumber(1)), pageId2)
		assert.Equal(t, page.NewPageId(fileId, page.PageNumber(2)), pageId3)
		assert.Equal(t, page.NewPageId(fileId, page.PageNumber(3)), disk.nextPageId)
	})
}

func TestReadPageData(t *testing.T) {
	t.Run("正常にデータを読み込める", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDisk(t)
		writeData := createDataBuffer()
		err := disk.WritePageData(pageId, writeData)
		assert.NoError(t, err)

		// WHEN
		readData := directio.AlignedBlock(directio.BlockSize)
		err = disk.ReadPageData(pageId, readData)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, writeData, readData)
	})

	t.Run("読み込むバッファのサイズが PageSize と異なる場合はエラー", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDisk(t)
		invalidData := make([]byte, page.PageSize-1)

		// WHEN
		err := disk.ReadPageData(pageId, invalidData)

		// THEN
		assert.Error(t, err)
	})

	t.Run("FileId が異なるページ ID で読み込むとエラーが返る", func(t *testing.T) {
		// GIVEN
		disk, _ := initDisk(t)
		wrongPageId := page.NewPageId(page.FileId(1), page.PageNumber(0))
		readData := directio.AlignedBlock(directio.BlockSize)

		// WHEN
		err := disk.ReadPageData(wrongPageId, readData)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid FileId")
	})

	t.Run("複数ページを書き込んで任意のページを読み込める", func(t *testing.T) {
		// GIVEN
		// 3 ページ分のデータを書き込む
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		disk, err := NewDisk(page.FileId(0), dbPath)
		assert.NoError(t, err)

		pages := make([][]byte, 3)
		pageIds := make([]page.PageId, 3)
		for i := range 3 {
			pageIds[i] = disk.AllocatePage()
			pages[i] = directio.AlignedBlock(directio.BlockSize)
			for j := range page.PageSize {
				pages[i][j] = byte((i*100 + j) % 256)
			}
			err := disk.WritePageData(pageIds[i], pages[i])
			assert.NoError(t, err)
		}

		// WHEN
		// 2 番目のページを読み込む
		readData := directio.AlignedBlock(directio.BlockSize)
		err = disk.ReadPageData(pageIds[1], readData)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pages[1], readData)
	})
}

func TestWritePageData(t *testing.T) {
	t.Run("正常にデータを書き込める", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDisk(t)
		writeData := createDataBuffer()

		// WHEN
		err := disk.WritePageData(pageId, writeData)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("書き込むデータのサイズが PageSize と異なる場合はエラー", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDisk(t)
		invalidData := make([]byte, page.PageSize+10)

		// WHEN
		err := disk.WritePageData(pageId, invalidData)

		// THEN
		assert.Error(t, err)
	})

	t.Run("FileId が異なるページ ID で書き込むとエラーが返る", func(t *testing.T) {
		// GIVEN
		disk, _ := initDisk(t)
		wrongPageId := page.NewPageId(page.FileId(1), page.PageNumber(0))
		writeData := createDataBuffer()

		// WHEN
		err := disk.WritePageData(wrongPageId, writeData)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid FileId")
	})

	t.Run("既に書き込んだページを上書きできる", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDisk(t)
		firstData := directio.AlignedBlock(directio.BlockSize)
		for i := range page.PageSize {
			firstData[i] = byte(0xAA)
		}
		err := disk.WritePageData(pageId, firstData)
		assert.NoError(t, err)

		// WHEN
		// 同じページに異なるデータを上書き
		secondData := directio.AlignedBlock(directio.BlockSize)
		for i := range page.PageSize {
			secondData[i] = byte(0xBB)
		}
		err = disk.WritePageData(pageId, secondData)
		assert.NoError(t, err)

		// THEN
		// 2 回目のデータが読み込める
		readData := directio.AlignedBlock(directio.BlockSize)
		err = disk.ReadPageData(pageId, readData)
		assert.NoError(t, err)
		assert.Equal(t, secondData, readData)
	})
}

func TestSync(t *testing.T) {
	t.Run("Sync が正常に実行できる", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDisk(t)
		writeData := createDataBuffer()
		err := disk.WritePageData(pageId, writeData)
		assert.NoError(t, err)

		// WHEN
		err = disk.Sync()

		// THEN
		assert.NoError(t, err)
	})
}

func TestClose(t *testing.T) {
	t.Run("Close が正常に実行できる", func(t *testing.T) {
		// GIVEN
		disk, _ := initDisk(t)

		// WHEN
		err := disk.Close()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("Close 後に ReadPageData を呼ぶとエラーが返る", func(t *testing.T) {
		// GIVEN
		disk, pageId := initDisk(t)
		writeData := createDataBuffer()
		err := disk.WritePageData(pageId, writeData)
		assert.NoError(t, err)

		err = disk.Close()
		assert.NoError(t, err)

		// WHEN
		readData := directio.AlignedBlock(directio.BlockSize)
		err = disk.ReadPageData(pageId, readData)

		// THEN
		assert.Error(t, err)
	})
}

func initDisk(t *testing.T) (*Disk, page.PageId) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "sample.db")
	disk, err := NewDisk(page.FileId(0), dbPath)
	if err != nil {
		t.Fatalf("Failed to open Disk: %v", err)
	}
	pageId := disk.AllocatePage()
	return disk, pageId
}

func createDataBuffer() []byte {
	writeData := directio.AlignedBlock(directio.BlockSize)
	for i := range page.PageSize {
		writeData[i] = byte(i % 256)
	}
	return writeData
}

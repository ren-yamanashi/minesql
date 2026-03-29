package storage

import (
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

func TestNewDisk(t *testing.T) {
	t.Run("正常に Disk が生成される", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		fileId := FileId(0)

		// WHEN
		dm, err := NewDisk(fileId, dbPath)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, NewPageId(fileId, PageNumber(0)), dm.nextPageId)
	})

	t.Run("無効なファイルが指定された場合はエラー", func(t *testing.T) {
		// GIVEN
		invalidPath := "/nonexistent/directory/file.db"
		fileId := FileId(0)

		// WHEN
		_, err := NewDisk(fileId, invalidPath)

		// THEN
		assert.Error(t, err)
	})

	t.Run("既存のデータファイルを開いた場合、nextPageId がファイルサイズに基づいて設定される", func(t *testing.T) {
		// GIVEN: 1 ページ分のデータを書き込んだファイルを用意
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		fileId := FileId(0)

		dm1, err := NewDisk(fileId, dbPath)
		assert.NoError(t, err)

		pageId := dm1.AllocatePage()
		writeData := createDataBuffer()
		err = dm1.WritePageData(pageId, writeData)
		assert.NoError(t, err)

		// WHEN: 同じパスで再度 Disk を生成
		dm2, err := NewDisk(fileId, dbPath)

		// THEN: nextPageId がファイルサイズから計算された値 (pageNumber=1) になる
		assert.NoError(t, err)
		assert.Equal(t, NewPageId(fileId, PageNumber(1)), dm2.nextPageId)
	})
}

func TestAllocatePage(t *testing.T) {
	t.Run("新しいページを順次割り当てられる", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		fileId := FileId(0)
		dm, err := NewDisk(fileId, dbPath)
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

func TestReadPageData(t *testing.T) {
	t.Run("正常にデータを読み込める", func(t *testing.T) {
		// GIVEN
		dm, pageId := initDisk(t)
		writeData := createDataBuffer()
		err := dm.WritePageData(pageId, writeData)
		assert.NoError(t, err)

		// WHEN
		readData := directio.AlignedBlock(directio.BlockSize)
		err = dm.ReadPageData(pageId, readData)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, writeData, readData)
	})

	t.Run("読み込むバッファのサイズが PAGE_SIZE と異なる場合はエラー", func(t *testing.T) {
		// GIVEN
		dm, pageId := initDisk(t)
		invalidData := make([]byte, PAGE_SIZE-1)

		// WHEN
		err := dm.ReadPageData(pageId, invalidData)

		// THEN
		assert.Error(t, err)
	})

	t.Run("FileId が異なるページ ID で読み込むとエラーが返る", func(t *testing.T) {
		// GIVEN: FileId(0) の Disk
		dm, _ := initDisk(t)

		// GIVEN: FileId(1) のページ ID を用意
		wrongPageId := NewPageId(FileId(1), PageNumber(0))
		readData := directio.AlignedBlock(directio.BlockSize)

		// WHEN
		err := dm.ReadPageData(wrongPageId, readData)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid FileId")
	})

	t.Run("複数ページを書き込んで任意のページを読み込める", func(t *testing.T) {
		// GIVEN: 3 ページ分のデータを書き込む
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		dm, err := NewDisk(FileId(0), dbPath)
		assert.NoError(t, err)

		pages := make([][]byte, 3)
		pageIds := make([]PageId, 3)
		for i := range 3 {
			pageIds[i] = dm.AllocatePage()
			pages[i] = directio.AlignedBlock(directio.BlockSize)
			for j := range PAGE_SIZE {
				pages[i][j] = byte((i*100 + j) % 256)
			}
			err := dm.WritePageData(pageIds[i], pages[i])
			assert.NoError(t, err)
		}

		// WHEN: 2 番目のページを読み込む
		readData := directio.AlignedBlock(directio.BlockSize)
		err = dm.ReadPageData(pageIds[1], readData)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, pages[1], readData)
	})
}

func TestWritePageData(t *testing.T) {
	t.Run("正常にデータを書き込める", func(t *testing.T) {
		// GIVEN
		dm, pageId := initDisk(t)
		writeData := createDataBuffer()

		// WHEN
		err := dm.WritePageData(pageId, writeData)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("書き込むデータのサイズが PAGE_SIZE と異なる場合はエラー", func(t *testing.T) {
		// GIVEN
		dm, pageId := initDisk(t)
		invalidData := make([]byte, PAGE_SIZE+10)

		// WHEN
		err := dm.WritePageData(pageId, invalidData)

		// THEN
		assert.Error(t, err)
	})

	t.Run("FileId が異なるページ ID で書き込むとエラーが返る", func(t *testing.T) {
		// GIVEN: FileId(0) の Disk
		dm, _ := initDisk(t)

		// GIVEN: FileId(1) のページ ID を用意
		wrongPageId := NewPageId(FileId(1), PageNumber(0))
		writeData := createDataBuffer()

		// WHEN
		err := dm.WritePageData(wrongPageId, writeData)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid FileId")
	})

	t.Run("既に書き込んだページを上書きできる", func(t *testing.T) {
		// GIVEN
		dm, pageId := initDisk(t)
		firstData := directio.AlignedBlock(directio.BlockSize)
		for i := range PAGE_SIZE {
			firstData[i] = byte(0xAA)
		}
		err := dm.WritePageData(pageId, firstData)
		assert.NoError(t, err)

		// WHEN: 同じページに異なるデータを上書き
		secondData := directio.AlignedBlock(directio.BlockSize)
		for i := range PAGE_SIZE {
			secondData[i] = byte(0xBB)
		}
		err = dm.WritePageData(pageId, secondData)
		assert.NoError(t, err)

		// THEN: 2 回目のデータが読み込める
		readData := directio.AlignedBlock(directio.BlockSize)
		err = dm.ReadPageData(pageId, readData)
		assert.NoError(t, err)
		assert.Equal(t, secondData, readData)
	})
}

func TestSync(t *testing.T) {
	t.Run("Sync が正常に実行できる", func(t *testing.T) {
		// GIVEN
		dm, pageId := initDisk(t)
		writeData := createDataBuffer()
		err := dm.WritePageData(pageId, writeData)
		assert.NoError(t, err)

		// WHEN
		err = dm.Sync()

		// THEN
		assert.NoError(t, err)
	})
}

func initDisk(t *testing.T) (*Disk, PageId) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "sample.db")
	dm, err := NewDisk(FileId(0), dbPath)
	if err != nil {
		t.Fatalf("Failed to open Disk: %v", err)
	}
	pageId := dm.AllocatePage()
	return dm, pageId
}

func createDataBuffer() []byte {
	writeData := directio.AlignedBlock(directio.BlockSize)
	for i := range PAGE_SIZE {
		writeData[i] = byte(i % 256)
	}
	return writeData
}

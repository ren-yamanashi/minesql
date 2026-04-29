package file

import (
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func newAlignedPage() []byte {
	return directio.AlignedBlock(page.PageSize)
}

func TestNewHeapFile(t *testing.T) {
	t.Run("新しいファイルで HeapFile を作成できる", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")

		// WHEN
		hf, err := NewHeapFile(0, path)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, hf)
		assert.NoError(t, hf.Close())
	})

	t.Run("既存データがあるファイルを開くと nextPageId がページ数から算出される", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf1, err := NewHeapFile(1, path)
		assert.NoError(t, err)
		data := newAlignedPage()
		assert.NoError(t, hf1.Write(0, data))
		assert.NoError(t, hf1.Write(1, data))
		assert.NoError(t, hf1.Close())

		// WHEN
		hf2, err := NewHeapFile(1, path)

		// THEN
		assert.NoError(t, err)
		nextId := hf2.AllocatePageId()
		assert.Equal(t, page.FileId(1), nextId.FileId)
		assert.Equal(t, page.PageNumber(2), nextId.PageNumber)
		assert.NoError(t, hf2.Close())
	})

	t.Run("存在しないディレクトリのパスの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		path := "/nonexistent/dir/test.db"

		// WHEN
		hf, err := NewHeapFile(0, path)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, hf)
	})
}

func TestAllocatePageId(t *testing.T) {
	t.Run("空ファイルの場合 PageNumber 0 から採番される", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(5, path)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, hf.Close()) }()

		// WHEN
		id := hf.AllocatePageId()

		// THEN
		assert.Equal(t, page.FileId(5), id.FileId)
		assert.Equal(t, page.PageNumber(0), id.PageNumber)
	})

	t.Run("連続で採番すると PageNumber がインクリメントされる", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(0, path)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, hf.Close()) }()

		// WHEN
		id1 := hf.AllocatePageId()
		id2 := hf.AllocatePageId()
		id3 := hf.AllocatePageId()

		// THEN
		assert.Equal(t, page.PageNumber(0), id1.PageNumber)
		assert.Equal(t, page.PageNumber(1), id2.PageNumber)
		assert.Equal(t, page.PageNumber(2), id3.PageNumber)
	})
}

func TestWrite(t *testing.T) {
	t.Run("PageSize のデータを書き込める", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(0, path)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, hf.Close()) }()
		data := newAlignedPage()
		data[0] = 0xFF

		// WHEN
		err = hf.Write(0, data)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("データサイズが PageSize でない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(0, path)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, hf.Close()) }()
		data := make([]byte, 100)

		// WHEN
		err = hf.Write(0, data)

		// THEN
		assert.ErrorIs(t, err, page.ErrInvalidDataSize)
	})
}

func TestRead(t *testing.T) {
	t.Run("データサイズが PageSize でない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(0, path)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, hf.Close()) }()
		data := make([]byte, 100)

		// WHEN
		err = hf.Read(0, data)

		// THEN
		assert.ErrorIs(t, err, page.ErrInvalidDataSize)
	})
}

func TestWriteAndRead(t *testing.T) {
	t.Run("書き込んだデータを正しく読み込める", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(0, path)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, hf.Close()) }()
		writeData := newAlignedPage()
		writeData[0] = 0xAA
		writeData[page.PageSize-1] = 0xBB
		assert.NoError(t, hf.Write(0, writeData))

		// WHEN
		readData := newAlignedPage()
		err = hf.Read(0, readData)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAA), readData[0])
		assert.Equal(t, byte(0xBB), readData[page.PageSize-1])
	})

	t.Run("複数ページに書き込んで各ページを正しく読み込める", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(0, path)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, hf.Close()) }()
		page0 := newAlignedPage()
		page0[0] = 0x01
		page1 := newAlignedPage()
		page1[0] = 0x02
		assert.NoError(t, hf.Write(0, page0))
		assert.NoError(t, hf.Write(1, page1))

		// WHEN
		read0 := newAlignedPage()
		read1 := newAlignedPage()
		err0 := hf.Read(0, read0)
		err1 := hf.Read(1, read1)

		// THEN
		assert.NoError(t, err0)
		assert.NoError(t, err1)
		assert.Equal(t, byte(0x01), read0[0])
		assert.Equal(t, byte(0x02), read1[0])
	})
}

func TestSync(t *testing.T) {
	t.Run("エラーなく同期できる", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(0, path)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, hf.Close()) }()

		// WHEN
		err = hf.Sync()

		// THEN
		assert.NoError(t, err)
	})
}

func TestClose(t *testing.T) {
	t.Run("エラーなくファイルを閉じることができる", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(0, path)
		assert.NoError(t, err)

		// WHEN
		err = hf.Close()

		// THEN
		assert.NoError(t, err)
	})
}

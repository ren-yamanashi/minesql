package file

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func newAlignedPage() []byte {
	return directio.AlignedBlock(page.Size)
}

func TestNewHeapFile(t *testing.T) {
	t.Run("新しいファイルで HeapFile を作成できる", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")

		// WHEN
		hf, err := NewHeapFile(path)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, hf)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })
	})

	t.Run("ファイルサイズがページサイズの倍数でない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		f, err := os.Create(path) //nolint:gosec // テスト用ファイル作成
		assert.NoError(t, err)
		_, err = f.Write(make([]byte, page.Size+1)) // ページサイズ + 1 バイト
		assert.NoError(t, err)
		assert.NoError(t, f.Close())

		// WHEN
		hf, err := NewHeapFile(path)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, hf)
		assert.Contains(t, err.Error(), "not a multiple of page size")
	})

	t.Run("存在しないディレクトリのパスの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		path := "/nonexistent/dir/test.db"

		// WHEN
		hf, err := NewHeapFile(path)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, hf)
	})
}

func TestPath(t *testing.T) {
	t.Run("コンストラクタに渡したパスを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })

		// WHEN
		got := hf.Path()

		// THEN
		assert.Equal(t, path, got)
	})

	t.Run("Close 済みでもパスを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		assert.NoError(t, hf.Close())

		// WHEN
		got := hf.Path()

		// THEN
		assert.Equal(t, path, got)
	})
}

func TestRead(t *testing.T) {
	t.Run("データサイズが PageSize でない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })
		data := make([]byte, 100)

		// WHEN
		err = hf.Read(0, data)

		// THEN
		assert.Error(t, err)
	})

	t.Run("nil データの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })

		// WHEN
		err = hf.Read(0, nil)

		// THEN
		assert.Error(t, err)
	})

	t.Run("存在しないページを読み込むと EOF エラーを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })
		data := newAlignedPage()

		// WHEN
		err = hf.Read(0, data)

		// THEN
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("Close 済みのファイルから読み込むとエラーを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		assert.NoError(t, hf.Close())
		data := newAlignedPage()

		// WHEN
		err = hf.Read(0, data)

		// THEN
		assert.Error(t, err)
	})

	t.Run("書き込んだデータを正しく読み込める", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })
		writeData := newAlignedPage()
		writeData[0] = 0xAA
		writeData[page.Size-1] = 0xBB
		assert.NoError(t, hf.Write(0, writeData))

		// WHEN
		readData := newAlignedPage()
		err = hf.Read(0, readData)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAA), readData[0])
		assert.Equal(t, byte(0xBB), readData[page.Size-1])
	})

	t.Run("複数ページに書き込んで各ページを正しく読み込める", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })
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

func TestWrite(t *testing.T) {
	t.Run("PageSize のデータを書き込める", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })
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
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })
		data := make([]byte, 100)

		// WHEN
		err = hf.Write(0, data)

		// THEN
		assert.Error(t, err)
	})

	t.Run("nil データの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })

		// WHEN
		err = hf.Write(0, nil)

		// THEN
		assert.Error(t, err)
	})

	t.Run("同じページに上書きできる", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })
		data1 := newAlignedPage()
		data1[0] = 0xAA
		assert.NoError(t, hf.Write(0, data1))
		data2 := newAlignedPage()
		data2[0] = 0xBB

		// WHEN
		err = hf.Write(0, data2)

		// THEN
		assert.NoError(t, err)
		readData := newAlignedPage()
		assert.NoError(t, hf.Read(0, readData))
		assert.Equal(t, byte(0xBB), readData[0])
	})

	t.Run("Close 済みのファイルに書き込むとエラーを返す", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		assert.NoError(t, hf.Close())
		data := newAlignedPage()

		// WHEN
		err = hf.Write(0, data)

		// THEN
		assert.Error(t, err)
	})
}

func TestSync(t *testing.T) {
	t.Run("エラーなく同期できる", func(t *testing.T) {
		// GIVEN
		path := filepath.Join(t.TempDir(), "test.db")
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, hf.Close()) })

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
		hf, err := NewHeapFile(path)
		assert.NoError(t, err)

		// WHEN
		err = hf.Close()

		// THEN
		assert.NoError(t, err)
	})
}

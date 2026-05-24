package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewId(t *testing.T) {
	t.Run("指定した FileId と PageNumber で Id を生成できる", func(t *testing.T) {
		// GIVEN
		fileId := FileId(1)
		pageNumber := PageNumber(2)

		// WHEN
		pageId := NewId(fileId, pageNumber)

		// THEN
		assert.Equal(t, fileId, pageId.FileId())
		assert.Equal(t, pageNumber, pageId.PageNumber())
	})

	t.Run("ゼロ値で Id を生成できる", func(t *testing.T) {
		// GIVEN / WHEN
		pageId := NewId(0, 0)

		// THEN
		assert.Equal(t, FileId(0), pageId.FileId())
		assert.Equal(t, PageNumber(0), pageId.PageNumber())
	})

	t.Run("最大値で Id を生成できる", func(t *testing.T) {
		// GIVEN / WHEN
		pageId := NewId(MaxFileId, MaxPageNumber)

		// THEN
		assert.Equal(t, MaxFileId, pageId.FileId())
		assert.Equal(t, MaxPageNumber, pageId.PageNumber())
	})
}

func TestIsInvalid(t *testing.T) {
	t.Run("InvalidId と一致する場合 true を返す", func(t *testing.T) {
		// GIVEN
		pageId := NewId(MaxFileId, MaxPageNumber)

		// WHEN
		result := pageId.IsInvalid()

		// THEN
		assert.True(t, result)
	})

	t.Run("InvalidId と一致しない場合 false を返す", func(t *testing.T) {
		// GIVEN
		pageId := NewId(1, 2)

		// WHEN
		result := pageId.IsInvalid()

		// THEN
		assert.False(t, result)
	})
}

func TestIdBytes(t *testing.T) {
	t.Run("BigEndian で 8 バイトのバイト列に変換できる", func(t *testing.T) {
		// GIVEN
		id := NewId(0x00000001, 0x00000002)

		// WHEN
		data := id.Bytes()

		// THEN
		expected := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}
		assert.Equal(t, expected, data)
	})

	t.Run("ゼロ値の Id を変換すると全て 0x00 になる", func(t *testing.T) {
		// GIVEN
		id := NewId(0, 0)

		// WHEN
		data := id.Bytes()

		// THEN
		expected := make([]byte, 8)
		assert.Equal(t, expected, data)
	})

	t.Run("最大値の Id を変換すると全て 0xFF になる", func(t *testing.T) {
		// GIVEN
		id := NewId(MaxFileId, MaxPageNumber)

		// WHEN
		data := id.Bytes()

		// THEN
		expected := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		assert.Equal(t, expected, data)
	})
}

func TestWriteAt(t *testing.T) {
	t.Run("指定した offset の位置に Id を書き込める", func(t *testing.T) {
		// GIVEN
		pageId := NewId(0x00000001, 0x00000002)
		data := make([]byte, 16)
		offset := 4

		// WHEN
		pageId.WriteAt(data, offset)

		// THEN
		expected := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}
		assert.Equal(t, expected, data[offset:offset+8])
	})

	t.Run("offset 0 の位置に Id を書き込める", func(t *testing.T) {
		// GIVEN
		pageId := NewId(0x00000001, 0x00000002)
		data := make([]byte, 8)

		// WHEN
		pageId.WriteAt(data, 0)

		// THEN
		expected := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}
		assert.Equal(t, expected, data)
	})

	t.Run("書き込み範囲外のデータは変更されない", func(t *testing.T) {
		// GIVEN
		pageId := NewId(0x00000001, 0x00000002)
		data := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEE, 0xFF}
		offset := 4

		// WHEN
		pageId.WriteAt(data, offset)

		// THEN
		assert.Equal(t, []byte{0xAA, 0xBB, 0xCC, 0xDD}, data[:4])
		assert.Equal(t, []byte{0xEE, 0xFF}, data[12:])
	})
}

func TestReadId(t *testing.T) {
	t.Run("指定した offset の位置から Id を読み込める", func(t *testing.T) {
		// GIVEN
		data := []byte{
			0x00, 0x00, 0x00, 0x00, // padding
			0x00, 0x00, 0x00, 0x03, // FileId = 3
			0x00, 0x00, 0x00, 0x07, // PageNumber = 7
		}
		offset := 4

		// WHEN
		pageId := ReadId(data, offset)

		// THEN
		assert.Equal(t, FileId(3), pageId.FileId())
		assert.Equal(t, PageNumber(7), pageId.PageNumber())
	})

	t.Run("offset 0 の位置から Id を読み込める", func(t *testing.T) {
		// GIVEN
		data := []byte{
			0x00, 0x00, 0x00, 0x05, // FileId = 5
			0x00, 0x00, 0x00, 0x0A, // PageNumber = 10
		}

		// WHEN
		pageId := ReadId(data, 0)

		// THEN
		assert.Equal(t, FileId(5), pageId.FileId())
		assert.Equal(t, PageNumber(10), pageId.PageNumber())
	})
}

func TestRestoreId(t *testing.T) {
	t.Run("8 バイトのデータから Id を復元できる", func(t *testing.T) {
		// GIVEN
		original := NewId(10, 20)
		data := original.Bytes()

		// WHEN
		restored, err := RestoreId(data)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, restored)
	})

	t.Run("データ長が 8 バイト未満の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := []byte{0x00, 0x00, 0x00}

		// WHEN
		pageId, err := RestoreId(data)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, InvalidId(), pageId)
	})

	t.Run("データ長が 8 バイト超過の場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := make([]byte, 9)

		// WHEN
		pageId, err := RestoreId(data)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, InvalidId(), pageId)
	})

	t.Run("空データの場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := []byte{}

		// WHEN
		pageId, err := RestoreId(data)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, InvalidId(), pageId)
	})
}

func TestWriteAndReadId(t *testing.T) {
	t.Run("WriteAt で書き込んだ Id を ReadId で復元できる", func(t *testing.T) {
		// GIVEN
		original := NewId(0xDEADBEEF, 0xCAFEBABE)
		data := make([]byte, 8)

		// WHEN
		original.WriteAt(data, 0)
		restored := ReadId(data, 0)

		// THEN
		assert.Equal(t, original, restored)
	})
}

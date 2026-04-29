package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPageId(t *testing.T) {
	t.Run("指定した FileId と PageNumber で PageId を生成できる", func(t *testing.T) {
		// GIVEN
		fileId := FileId(1)
		pageNumber := PageNumber(2)

		// WHEN
		pageId := NewPageId(fileId, pageNumber)

		// THEN
		assert.Equal(t, fileId, pageId.FileId)
		assert.Equal(t, pageNumber, pageId.PageNumber)
	})
}

func TestIsInvalid(t *testing.T) {
	t.Run("InvalidPageId と一致する場合 true を返す", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(MaxFileId, MaxPageNumber)

		// WHEN
		result := pageId.IsInvalid()

		// THEN
		assert.True(t, result)
	})

	t.Run("InvalidPageId と一致しない場合 false を返す", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(1, 2)

		// WHEN
		result := pageId.IsInvalid()

		// THEN
		assert.False(t, result)
	})
}

func TestToBytes(t *testing.T) {
	t.Run("BigEndian で 8 バイトのバイト列に変換できる", func(t *testing.T) {
		// GIVEN
		id := NewPageId(0x00000001, 0x00000002)

		// WHEN
		data := id.ToBytes()

		// THEN
		expected := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}
		assert.Equal(t, expected, data)
	})
}

func TestWriteTo(t *testing.T) {
	t.Run("指定した offset の位置に PageId を書き込める", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(0x00000001, 0x00000002)
		data := make([]byte, 16)
		offset := 4

		// WHEN
		pageId.WriteTo(data, offset)

		// THEN
		expected := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}
		assert.Equal(t, expected, data[offset:offset+8])
	})
}

func TestReadPageId(t *testing.T) {
	t.Run("指定した offset の位置から PageId を読み込める", func(t *testing.T) {
		// GIVEN
		data := []byte{
			0x00, 0x00, 0x00, 0x00, // padding
			0x00, 0x00, 0x00, 0x03, // FileId = 3
			0x00, 0x00, 0x00, 0x07, // PageNumber = 7
		}
		offset := 4

		// WHEN
		pageId := ReadPageId(data, offset)

		// THEN
		assert.Equal(t, FileId(3), pageId.FileId)
		assert.Equal(t, PageNumber(7), pageId.PageNumber)
	})
}

func TestRestorePageId(t *testing.T) {
	t.Run("8 バイトのデータから PageId を復元できる", func(t *testing.T) {
		// GIVEN
		original := NewPageId(10, 20)
		data := original.ToBytes()

		// WHEN
		restored, err := RestorePageId(data)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, restored)
	})

	t.Run("データ長が 8 バイトでない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := []byte{0x00, 0x00, 0x00}

		// WHEN
		pageId, err := RestorePageId(data)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, InvalidPageId, pageId)
	})
}

func TestWriteAndReadPageId(t *testing.T) {
	t.Run("WriteTo で書き込んだ PageId を ReaPagedId で復元できる", func(t *testing.T) {
		// GIVEN
		original := NewPageId(0xDEADBEEF, 0xCAFEBABE)
		data := make([]byte, 8)

		// WHEN
		original.WriteTo(data, 0)
		restored := ReadPageId(data, 0)

		// THEN
		assert.Equal(t, original, restored)
	})
}

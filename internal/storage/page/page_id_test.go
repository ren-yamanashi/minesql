package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewId(t *testing.T) {
	t.Run("指定した FileId と PageNumber で Id を生成できる", func(t *testing.T) {
		// GIVEN
		fileId := uint32(1)
		pageNum := uint32(2)

		// WHEN
		id := NewId(fileId, pageNum)

		// THEN
		assert.Equal(t, fileId, id.FileId)
		assert.Equal(t, pageNum, id.PageNumber)
	})
}

func TestIsInvalid(t *testing.T) {
	t.Run("InvalidId と一致する場合 true を返す", func(t *testing.T) {
		// GIVEN
		id := NewId(MaxFileId, MaxPageNumber)

		// WHEN
		result := id.IsInvalid()

		// THEN
		assert.True(t, result)
	})

	t.Run("InvalidId と一致しない場合 false を返す", func(t *testing.T) {
		// GIVEN
		id := NewId(1, 2)

		// WHEN
		result := id.IsInvalid()

		// THEN
		assert.False(t, result)
	})
}

func TestToBytes(t *testing.T) {
	t.Run("BigEndian で 8 バイトのバイト列に変換できる", func(t *testing.T) {
		// GIVEN
		id := NewId(0x00000001, 0x00000002)

		// WHEN
		data := id.ToBytes()

		// THEN
		expected := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}
		assert.Equal(t, expected, data)
	})
}

func TestWriteTo(t *testing.T) {
	t.Run("指定した offset の位置に Id を書き込める", func(t *testing.T) {
		// GIVEN
		id := NewId(0x00000001, 0x00000002)
		data := make([]byte, 16)
		offset := 4

		// WHEN
		id.WriteTo(data, offset)

		// THEN
		expected := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02}
		assert.Equal(t, expected, data[offset:offset+8])
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
		id := ReadId(data, offset)

		// THEN
		assert.Equal(t, uint32(3), id.FileId)
		assert.Equal(t, uint32(7), id.PageNumber)
	})
}

func TestRestoreId(t *testing.T) {
	t.Run("8 バイトのデータから Id を復元できる", func(t *testing.T) {
		// GIVEN
		original := NewId(10, 20)
		data := original.ToBytes()

		// WHEN
		restored, err := RestoreId(data)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, original, restored)
	})

	t.Run("データ長が 8 バイトでない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		data := []byte{0x00, 0x00, 0x00}

		// WHEN
		id, err := RestoreId(data)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, InvalidId, id)
	})
}

func TestWriteAndReadId(t *testing.T) {
	t.Run("WriteTo で書き込んだ Id を ReadId で復元できる", func(t *testing.T) {
		// GIVEN
		original := NewId(0xDEADBEEF, 0xCAFEBABE)
		data := make([]byte, 8)

		// WHEN
		original.WriteTo(data, 0)
		restored := ReadId(data, 0)

		// THEN
		assert.Equal(t, original, restored)
	})
}

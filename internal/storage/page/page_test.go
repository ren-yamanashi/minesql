package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPageId(t *testing.T) {
	t.Run("PageId を作成できる", func(t *testing.T) {
		// GIVEN
		fileId := FileId(1)
		pageNumber := PageNumber(2)

		// WHEN
		pageId := NewPageId(fileId, pageNumber)

		// THEN
		assert.Equal(t, fileId, pageId.FileId)
		assert.Equal(t, pageNumber, pageId.PageNumber)
	})

	t.Run("FileId と PageNumber が 0 の PageId を作成できる", func(t *testing.T) {
		// GIVEN
		fileId := FileId(0)
		pageNumber := PageNumber(0)

		// WHEN
		pageId := NewPageId(fileId, pageNumber)

		// THEN
		assert.Equal(t, fileId, pageId.FileId)
		assert.Equal(t, pageNumber, pageId.PageNumber)
	})

	t.Run("FileId と PageNumber が最大値の PageId を作成できる", func(t *testing.T) {
		// GIVEN
		fileId := FileId(0xFFFFFFFF)
		pageNumber := PageNumber(0xFFFFFFFF)

		// WHEN
		pageId := NewPageId(fileId, pageNumber)

		// THEN
		assert.Equal(t, fileId, pageId.FileId)
		assert.Equal(t, pageNumber, pageId.PageNumber)
	})
}

func TestEquals(t *testing.T) {
	t.Run("同じ FileId と PageNumber を持つ PageId は等しい", func(t *testing.T) {
		// GIVEN
		pageId1 := NewPageId(1, 2)
		pageId2 := NewPageId(1, 2)

		// WHEN
		result := pageId1.Equals(pageId2)

		// THEN
		assert.True(t, result)
	})

	t.Run("FileId が異なる PageId は等しくない", func(t *testing.T) {
		// GIVEN
		pageId1 := NewPageId(1, 2)
		pageId2 := NewPageId(2, 2)

		// WHEN
		result := pageId1.Equals(pageId2)

		// THEN
		assert.False(t, result)
	})

	t.Run("PageNumber が異なる PageId は等しくない", func(t *testing.T) {
		// GIVEN
		pageId1 := NewPageId(1, 2)
		pageId2 := NewPageId(1, 3)

		// WHEN
		result := pageId1.Equals(pageId2)

		// THEN
		assert.False(t, result)
	})

	t.Run("FileId と PageNumber の両方が異なる PageId は等しくない", func(t *testing.T) {
		// GIVEN
		pageId1 := NewPageId(1, 2)
		pageId2 := NewPageId(3, 4)

		// WHEN
		result := pageId1.Equals(pageId2)

		// THEN
		assert.False(t, result)
	})
}

func TestIsInvalid(t *testing.T) {
	t.Run("INVALID_PAGE_ID は無効と判定される", func(t *testing.T) {
		// GIVEN
		pageId := INVALID_PAGE_ID

		// WHEN
		result := pageId.IsInvalid()

		// THEN
		assert.True(t, result)
	})

	t.Run("有効な PageId は無効と判定されない", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(1, 2)

		// WHEN
		result := pageId.IsInvalid()

		// THEN
		assert.False(t, result)
	})

	t.Run("FileId だけが最大値の PageId は無効と判定されない", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(0xFFFFFFFF, 0)

		// WHEN
		result := pageId.IsInvalid()

		// THEN
		assert.False(t, result)
	})

	t.Run("PageNumber だけが最大値の PageId は無効と判定されない", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(0, 0xFFFFFFFF)

		// WHEN
		result := pageId.IsInvalid()

		// THEN
		assert.False(t, result)
	})
}

func TestToBytes(t *testing.T) {
	t.Run("PageId をバイト列に変換できる", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(0x12345678, 0xABCDEF00)

		// WHEN
		bytes := pageId.ToBytes()

		// THEN
		assert.Equal(t, 8, len(bytes))
		// Little Endian で格納されているか確認
		assert.Equal(t, byte(0x78), bytes[0])
		assert.Equal(t, byte(0x56), bytes[1])
		assert.Equal(t, byte(0x34), bytes[2])
		assert.Equal(t, byte(0x12), bytes[3])
		assert.Equal(t, byte(0x00), bytes[4])
		assert.Equal(t, byte(0xEF), bytes[5])
		assert.Equal(t, byte(0xCD), bytes[6])
		assert.Equal(t, byte(0xAB), bytes[7])
	})

	t.Run("FileId と PageNumber が 0 の PageId をバイト列に変換できる", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(0, 0)

		// WHEN
		bytes := pageId.ToBytes()

		// THEN
		assert.Equal(t, 8, len(bytes))
		assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0}, bytes)
	})
}

func TestPageIdFromBytes(t *testing.T) {
	t.Run("バイト列から PageId を復元できる", func(t *testing.T) {
		// GIVEN
		bytes := []byte{0x78, 0x56, 0x34, 0x12, 0x00, 0xEF, 0xCD, 0xAB}

		// WHEN
		pageId := PageIdFromBytes(bytes)

		// THEN
		assert.Equal(t, FileId(0x12345678), pageId.FileId)
		assert.Equal(t, PageNumber(0xABCDEF00), pageId.PageNumber)
	})

	t.Run("すべて 0 のバイト列から PageId を復元できる", func(t *testing.T) {
		// GIVEN
		bytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}

		// WHEN
		pageId := PageIdFromBytes(bytes)

		// THEN
		assert.Equal(t, FileId(0), pageId.FileId)
		assert.Equal(t, PageNumber(0), pageId.PageNumber)
	})

	t.Run("8 バイト以外のデータから復元しようとすると panic", func(t *testing.T) {
		// GIVEN
		bytes := []byte{0, 1, 2, 3, 4, 5, 6}

		// WHEN & THEN
		assert.Panics(t, func() {
			PageIdFromBytes(bytes)
		})
	})
}

func TestToBytesAndPageIdFromBytes(t *testing.T) {
	t.Run("ToBytes と PageIdFromBytes は可逆", func(t *testing.T) {
		// GIVEN
		original := NewPageId(123, 456)

		// WHEN
		bytes := original.ToBytes()
		restored := PageIdFromBytes(bytes)

		// THEN
		assert.True(t, original.Equals(restored))
	})
}

func TestWriteTo(t *testing.T) {
	t.Run("PageId を指定位置に書き込める", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(0x12345678, 0xABCDEF00)
		data := make([]byte, 16)

		// WHEN
		pageId.WriteTo(data, 4)

		// THEN
		// offset 4 から 8 バイト分に PageId が書き込まれているか確認
		assert.Equal(t, byte(0x78), data[4])
		assert.Equal(t, byte(0x56), data[5])
		assert.Equal(t, byte(0x34), data[6])
		assert.Equal(t, byte(0x12), data[7])
		assert.Equal(t, byte(0x00), data[8])
		assert.Equal(t, byte(0xEF), data[9])
		assert.Equal(t, byte(0xCD), data[10])
		assert.Equal(t, byte(0xAB), data[11])
		// 他の部分は 0 のまま
		assert.Equal(t, byte(0), data[0])
		assert.Equal(t, byte(0), data[3])
		assert.Equal(t, byte(0), data[12])
		assert.Equal(t, byte(0), data[15])
	})

	t.Run("offset 0 に PageId を書き込める", func(t *testing.T) {
		// GIVEN
		pageId := NewPageId(1, 2)
		data := make([]byte, 16)

		// WHEN
		pageId.WriteTo(data, 0)

		// THEN
		// offset 0 から 8 バイト分に PageId が書き込まれているか確認
		assert.Equal(t, byte(1), data[0])
		assert.Equal(t, byte(0), data[1])
		assert.Equal(t, byte(0), data[2])
		assert.Equal(t, byte(0), data[3])
		assert.Equal(t, byte(2), data[4])
		assert.Equal(t, byte(0), data[5])
		assert.Equal(t, byte(0), data[6])
		assert.Equal(t, byte(0), data[7])
	})
}

func TestReadPageIdFrom(t *testing.T) {
	t.Run("指定位置から PageId を読み取れる", func(t *testing.T) {
		// GIVEN
		data := []byte{0, 0, 0, 0, 0x78, 0x56, 0x34, 0x12, 0x00, 0xEF, 0xCD, 0xAB, 0, 0, 0, 0}

		// WHEN
		pageId := ReadPageIdFrom(data, 4)

		// THEN
		assert.Equal(t, FileId(0x12345678), pageId.FileId)
		assert.Equal(t, PageNumber(0xABCDEF00), pageId.PageNumber)
	})

	t.Run("offset 0 から PageId を読み取れる", func(t *testing.T) {
		// GIVEN
		data := []byte{1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

		// WHEN
		pageId := ReadPageIdFrom(data, 0)

		// THEN
		assert.Equal(t, FileId(1), pageId.FileId)
		assert.Equal(t, PageNumber(2), pageId.PageNumber)
	})
}

func TestWriteToAndReadPageIdFrom(t *testing.T) {
	t.Run("WriteTo と ReadPageIdFrom は可逆", func(t *testing.T) {
		// GIVEN
		original := NewPageId(999, 888)
		data := make([]byte, 16)

		// WHEN
		original.WriteTo(data, 3)
		restored := ReadPageIdFrom(data, 3)

		// THEN
		assert.True(t, original.Equals(restored))
	})
}

package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestIndexRecordEncode(t *testing.T) {
	t.Run("インデックスレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		ir := NewIndexRecord(page.FileId(1), IndexId(10), "idx_name", IndexTypeNonUnique, 2)

		// WHEN
		record := ir.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("プライマリインデックスをエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewIndexRecord(page.FileId(1), IndexId(1), "PRIMARY", IndexTypePrimary, 1)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, original.FileId, decoded.FileId)
		assert.Equal(t, original.IndexId, decoded.IndexId)
		assert.Equal(t, original.Name, decoded.Name)
		assert.Equal(t, IndexTypePrimary, decoded.IndexType)
		assert.Equal(t, 1, decoded.NumOfCol)
	})

	t.Run("ユニークインデックスをエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewIndexRecord(page.FileId(2), IndexId(5), "idx_email", IndexTypeUnique, 1)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, original.FileId, decoded.FileId)
		assert.Equal(t, original.IndexId, decoded.IndexId)
		assert.Equal(t, original.Name, decoded.Name)
		assert.Equal(t, IndexTypeUnique, decoded.IndexType)
		assert.Equal(t, 1, decoded.NumOfCol)
	})

	t.Run("非ユニークインデックスをエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewIndexRecord(page.FileId(3), IndexId(20), "idx_age", IndexTypeNonUnique, 1)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, original.FileId, decoded.FileId)
		assert.Equal(t, original.IndexId, decoded.IndexId)
		assert.Equal(t, original.Name, decoded.Name)
		assert.Equal(t, IndexTypeNonUnique, decoded.IndexType)
		assert.Equal(t, 1, decoded.NumOfCol)
	})

	t.Run("複合インデックスのカラム数をエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewIndexRecord(page.FileId(1), IndexId(3), "idx_composite", IndexTypeNonUnique, 3)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, 3, decoded.NumOfCol)
	})

	t.Run("FileId と IndexId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewIndexRecord(page.FileId(0), IndexId(0), "idx", IndexTypePrimary, 1)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, page.FileId(0), decoded.FileId)
		assert.Equal(t, IndexId(0), decoded.IndexId)
		assert.Equal(t, 1, decoded.NumOfCol)
	})
}

func TestDecodeIndexRecord(t *testing.T) {
	t.Run("エンコード済みレコードから FileId とインデックス ID を復元できる", func(t *testing.T) {
		// GIVEN
		ir := NewIndexRecord(page.FileId(42), IndexId(100), "idx_test", IndexTypeUnique, 2)
		record := ir.encode()

		// WHEN
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, page.FileId(42), decoded.FileId)
		assert.Equal(t, IndexId(100), decoded.IndexId)
	})

	t.Run("エンコード済みレコードからインデックス名と種類とカラム数を復元できる", func(t *testing.T) {
		// GIVEN
		ir := NewIndexRecord(page.FileId(1), IndexId(1), "idx_composite", IndexTypeNonUnique, 3)
		record := ir.encode()

		// WHEN
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, "idx_composite", decoded.Name)
		assert.Equal(t, IndexTypeNonUnique, decoded.IndexType)
		assert.Equal(t, 3, decoded.NumOfCol)
	})
}

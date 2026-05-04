package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestIndexRecordEncode(t *testing.T) {
	t.Run("インデックスレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		ir := newIndexRecord(page.FileId(1), "idx_name", IndexId(10), IndexTypeNonUnique, 2, page.NewPageId(page.FileId(1), page.PageNumber(0)))

		// WHEN
		record := ir.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("プライマリインデックスをエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newIndexRecord(page.FileId(1), PrimaryIndexName, IndexId(1), IndexTypePrimary, 1, page.NewPageId(page.FileId(1), page.PageNumber(0)))

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, original.FileId, decoded.FileId)
		assert.Equal(t, original.IndexId, decoded.IndexId)
		assert.Equal(t, original.Name, decoded.Name)
		assert.Equal(t, IndexTypePrimary, decoded.IndexType)
		assert.Equal(t, 1, decoded.NumOfCol)
		assert.Equal(t, original.MetaPageId, decoded.MetaPageId)
	})

	t.Run("ユニークインデックスをエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newIndexRecord(page.FileId(2), "idx_email", IndexId(5), IndexTypeUnique, 1, page.NewPageId(page.FileId(2), page.PageNumber(0)))

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
		original := newIndexRecord(page.FileId(3), "idx_age", IndexId(20), IndexTypeNonUnique, 1, page.NewPageId(page.FileId(3), page.PageNumber(0)))

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
		original := newIndexRecord(page.FileId(1), "idx_composite", IndexId(3), IndexTypeNonUnique, 3, page.NewPageId(page.FileId(1), page.PageNumber(0)))

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, 3, decoded.NumOfCol)
	})

	t.Run("FileId と IndexId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newIndexRecord(page.FileId(0), "idx", IndexId(0), IndexTypePrimary, 1, page.NewPageId(page.FileId(0), page.PageNumber(0)))

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, page.FileId(0), decoded.FileId)
		assert.Equal(t, IndexId(0), decoded.IndexId)
		assert.Equal(t, 1, decoded.NumOfCol)
	})

	t.Run("MetaPageId をエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		metaPageId := page.NewPageId(page.FileId(5), page.PageNumber(10))
		original := newIndexRecord(page.FileId(1), "idx_test", IndexId(1), IndexTypePrimary, 1, metaPageId)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, metaPageId, decoded.MetaPageId)
	})
}

func TestDecodeIndexRecord(t *testing.T) {
	t.Run("エンコード済みレコードから FileId とインデックス ID を復元できる", func(t *testing.T) {
		// GIVEN
		ir := newIndexRecord(page.FileId(42), "idx_test", IndexId(100), IndexTypeUnique, 2, page.NewPageId(page.FileId(42), page.PageNumber(0)))
		record := ir.encode()

		// WHEN
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, page.FileId(42), decoded.FileId)
		assert.Equal(t, IndexId(100), decoded.IndexId)
	})

	t.Run("エンコード済みレコードからインデックス名と種類とカラム数を復元できる", func(t *testing.T) {
		// GIVEN
		ir := newIndexRecord(page.FileId(1), "idx_composite", IndexId(1), IndexTypeNonUnique, 3, page.NewPageId(page.FileId(1), page.PageNumber(0)))
		record := ir.encode()

		// WHEN
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, "idx_composite", decoded.Name)
		assert.Equal(t, IndexTypeNonUnique, decoded.IndexType)
		assert.Equal(t, 3, decoded.NumOfCol)
	})
}

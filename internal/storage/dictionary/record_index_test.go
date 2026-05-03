package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestIndexRecordEncode(t *testing.T) {
	t.Run("インデックスレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		ir := newIndexRecord(page.FileId(1), IndexId(10), "idx_name", IndexTypeNonUnique)

		// WHEN
		record := ir.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("プライマリインデックスをエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newIndexRecord(page.FileId(1), IndexId(1), "PRIMARY", IndexTypePrimary)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, original.fileId, decoded.fileId)
		assert.Equal(t, original.indexId, decoded.indexId)
		assert.Equal(t, original.name, decoded.name)
		assert.Equal(t, IndexTypePrimary, decoded.indexType)
	})

	t.Run("ユニークインデックスをエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newIndexRecord(page.FileId(2), IndexId(5), "idx_email", IndexTypeUnique)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, original.fileId, decoded.fileId)
		assert.Equal(t, original.indexId, decoded.indexId)
		assert.Equal(t, original.name, decoded.name)
		assert.Equal(t, IndexTypeUnique, decoded.indexType)
	})

	t.Run("非ユニークインデックスをエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newIndexRecord(page.FileId(3), IndexId(20), "idx_age", IndexTypeNonUnique)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, original.fileId, decoded.fileId)
		assert.Equal(t, original.indexId, decoded.indexId)
		assert.Equal(t, original.name, decoded.name)
		assert.Equal(t, IndexTypeNonUnique, decoded.indexType)
	})

	t.Run("FileId と IndexId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newIndexRecord(page.FileId(0), IndexId(0), "idx", IndexTypePrimary)

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, page.FileId(0), decoded.fileId)
		assert.Equal(t, IndexId(0), decoded.indexId)
	})
}

func TestDecodeIndexRecord(t *testing.T) {
	t.Run("エンコード済みレコードから FileId とインデックス ID を復元できる", func(t *testing.T) {
		// GIVEN
		ir := newIndexRecord(page.FileId(42), IndexId(100), "idx_test", IndexTypeUnique)
		record := ir.encode()

		// WHEN
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, page.FileId(42), decoded.fileId)
		assert.Equal(t, IndexId(100), decoded.indexId)
	})

	t.Run("エンコード済みレコードからインデックス名と種類を復元できる", func(t *testing.T) {
		// GIVEN
		ir := newIndexRecord(page.FileId(1), IndexId(1), "idx_composite", IndexTypeNonUnique)
		record := ir.encode()

		// WHEN
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, "idx_composite", decoded.name)
		assert.Equal(t, IndexTypeNonUnique, decoded.indexType)
	})
}

package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestIndexRecordEncode(t *testing.T) {
	t.Run("インデックスレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		ir := IndexRecord{FileId: page.FileId(1), Name: "idx_name", IndexId: IndexId(10), IndexType: IndexTypeNonUnique, NumOfCol: 2, MetaPageId: page.NewPageId(page.FileId(1), page.PageNumber(0))}

		// WHEN
		record := ir.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("プライマリインデックスをエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := IndexRecord{FileId: page.FileId(1), Name: PrimaryIndexName, IndexId: IndexId(1), IndexType: IndexTypePrimary, NumOfCol: 1, MetaPageId: page.NewPageId(page.FileId(1), page.PageNumber(0))}

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
		original := IndexRecord{FileId: page.FileId(2), Name: "idx_email", IndexId: IndexId(5), IndexType: IndexTypeUnique, NumOfCol: 1, MetaPageId: page.NewPageId(page.FileId(2), page.PageNumber(0))}

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
		original := IndexRecord{FileId: page.FileId(3), Name: "idx_age", IndexId: IndexId(20), IndexType: IndexTypeNonUnique, NumOfCol: 1, MetaPageId: page.NewPageId(page.FileId(3), page.PageNumber(0))}

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
		original := IndexRecord{FileId: page.FileId(1), Name: "idx_composite", IndexId: IndexId(3), IndexType: IndexTypeNonUnique, NumOfCol: 3, MetaPageId: page.NewPageId(page.FileId(1), page.PageNumber(0))}

		// WHEN
		record := original.encode()
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, 3, decoded.NumOfCol)
	})

	t.Run("FileId と IndexId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := IndexRecord{FileId: page.FileId(0), Name: "idx", IndexId: IndexId(0), IndexType: IndexTypePrimary, NumOfCol: 1, MetaPageId: page.NewPageId(page.FileId(0), page.PageNumber(0))}

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
		original := IndexRecord{FileId: page.FileId(1), Name: "idx_test", IndexId: IndexId(1), IndexType: IndexTypePrimary, NumOfCol: 1, MetaPageId: metaPageId}

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
		ir := IndexRecord{FileId: page.FileId(42), Name: "idx_test", IndexId: IndexId(100), IndexType: IndexTypeUnique, NumOfCol: 2, MetaPageId: page.NewPageId(page.FileId(42), page.PageNumber(0))}
		record := ir.encode()

		// WHEN
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, page.FileId(42), decoded.FileId)
		assert.Equal(t, IndexId(100), decoded.IndexId)
	})

	t.Run("エンコード済みレコードからインデックス名と種類とカラム数を復元できる", func(t *testing.T) {
		// GIVEN
		ir := IndexRecord{FileId: page.FileId(1), Name: "idx_composite", IndexId: IndexId(1), IndexType: IndexTypeNonUnique, NumOfCol: 3, MetaPageId: page.NewPageId(page.FileId(1), page.PageNumber(0))}
		record := ir.encode()

		// WHEN
		decoded := decodeIndexRecord(record)

		// THEN
		assert.Equal(t, "idx_composite", decoded.Name)
		assert.Equal(t, IndexTypeNonUnique, decoded.IndexType)
		assert.Equal(t, 3, decoded.NumOfCol)
	})
}

package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestTableRecordEncode(t *testing.T) {
	t.Run("テーブルレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		tr := newTableRecord(page.FileId(1), "users", 3)

		// WHEN
		record := tr.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("エンコードした結果をデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := newTableRecord(page.FileId(1), "users", 3)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, original.fileId, decoded.fileId)
		assert.Equal(t, original.name, decoded.name)
		assert.Equal(t, original.numOfCol, decoded.numOfCol)
	})

	t.Run("カラム数が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newTableRecord(page.FileId(2), "empty_table", 0)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, original.fileId, decoded.fileId)
		assert.Equal(t, original.name, decoded.name)
		assert.Equal(t, original.numOfCol, decoded.numOfCol)
	})

	t.Run("長いテーブル名でも正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newTableRecord(page.FileId(3), "very_long_table_name_for_testing", 10)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, original.name, decoded.name)
	})

	t.Run("FileId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newTableRecord(page.FileId(0), "t", 1)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, page.FileId(0), decoded.fileId)
		assert.Equal(t, "t", decoded.name)
		assert.Equal(t, 1, decoded.numOfCol)
	})
}

func TestDecodeTableRecord(t *testing.T) {
	t.Run("エンコード済みレコードから FileId を復元できる", func(t *testing.T) {
		// GIVEN
		tr := newTableRecord(page.FileId(42), "orders", 5)
		record := tr.encode()

		// WHEN
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, page.FileId(42), decoded.fileId)
	})

	t.Run("エンコード済みレコードからテーブル名とカラム数を復元できる", func(t *testing.T) {
		// GIVEN
		tr := newTableRecord(page.FileId(1), "products", 7)
		record := tr.encode()

		// WHEN
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, "products", decoded.name)
		assert.Equal(t, 7, decoded.numOfCol)
	})
}

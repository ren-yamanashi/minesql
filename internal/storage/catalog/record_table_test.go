package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestTableRecordEncode(t *testing.T) {
	t.Run("テーブルレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		tr := NewTableRecord(page.FileId(1), "users", 3)

		// WHEN
		record := tr.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("エンコードした結果をデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := NewTableRecord(page.FileId(1), "users", 3)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, original.FileId, decoded.FileId)
		assert.Equal(t, original.Name, decoded.Name)
		assert.Equal(t, original.NumOfCol, decoded.NumOfCol)
	})

	t.Run("カラム数が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewTableRecord(page.FileId(2), "empty_table", 0)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, original.FileId, decoded.FileId)
		assert.Equal(t, original.Name, decoded.Name)
		assert.Equal(t, original.NumOfCol, decoded.NumOfCol)
	})

	t.Run("長いテーブル名でも正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewTableRecord(page.FileId(3), "very_long_table_name_for_testing", 10)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, original.Name, decoded.Name)
	})

	t.Run("FileId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewTableRecord(page.FileId(0), "t", 1)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, page.FileId(0), decoded.FileId)
		assert.Equal(t, "t", decoded.Name)
		assert.Equal(t, 1, decoded.NumOfCol)
	})
}

func TestDecodeTableRecord(t *testing.T) {
	t.Run("エンコード済みレコードから FileId を復元できる", func(t *testing.T) {
		// GIVEN
		tr := NewTableRecord(page.FileId(42), "orders", 5)
		record := tr.encode()

		// WHEN
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, page.FileId(42), decoded.FileId)
	})

	t.Run("エンコード済みレコードからテーブル名とカラム数を復元できる", func(t *testing.T) {
		// GIVEN
		tr := NewTableRecord(page.FileId(1), "products", 7)
		record := tr.encode()

		// WHEN
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, "products", decoded.Name)
		assert.Equal(t, 7, decoded.NumOfCol)
	})
}

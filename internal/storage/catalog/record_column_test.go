package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestColumnRecordEncode(t *testing.T) {
	t.Run("カラムレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		cr := newColumnRecord(page.FileId(1), "name", 0)

		// WHEN
		record := cr.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("エンコードした結果をデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := newColumnRecord(page.FileId(1), "email", 2)

		// WHEN
		record := original.encode()
		decoded := decodeColumnRecord(record)

		// THEN
		assert.Equal(t, original.fileId, decoded.fileId)
		assert.Equal(t, original.name, decoded.name)
		assert.Equal(t, original.pos, decoded.pos)
	})

	t.Run("カラム位置が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newColumnRecord(page.FileId(2), "id", 0)

		// WHEN
		record := original.encode()
		decoded := decodeColumnRecord(record)

		// THEN
		assert.Equal(t, page.FileId(2), decoded.fileId)
		assert.Equal(t, "id", decoded.name)
		assert.Equal(t, 0, decoded.pos)
	})

	t.Run("FileId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newColumnRecord(page.FileId(0), "col", 5)

		// WHEN
		record := original.encode()
		decoded := decodeColumnRecord(record)

		// THEN
		assert.Equal(t, page.FileId(0), decoded.fileId)
		assert.Equal(t, "col", decoded.name)
		assert.Equal(t, 5, decoded.pos)
	})

	t.Run("長いカラム名でも正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := newColumnRecord(page.FileId(3), "very_long_column_name_for_testing", 10)

		// WHEN
		record := original.encode()
		decoded := decodeColumnRecord(record)

		// THEN
		assert.Equal(t, "very_long_column_name_for_testing", decoded.name)
	})
}

func TestDecodeColumnRecord(t *testing.T) {
	t.Run("エンコード済みレコードから FileId とカラム名を復元できる", func(t *testing.T) {
		// GIVEN
		cr := newColumnRecord(page.FileId(42), "age", 3)
		record := cr.encode()

		// WHEN
		decoded := decodeColumnRecord(record)

		// THEN
		assert.Equal(t, page.FileId(42), decoded.fileId)
		assert.Equal(t, "age", decoded.name)
	})

	t.Run("エンコード済みレコードからカラム位置を復元できる", func(t *testing.T) {
		// GIVEN
		cr := newColumnRecord(page.FileId(1), "status", 7)
		record := cr.encode()

		// WHEN
		decoded := decodeColumnRecord(record)

		// THEN
		assert.Equal(t, 7, decoded.pos)
	})
}

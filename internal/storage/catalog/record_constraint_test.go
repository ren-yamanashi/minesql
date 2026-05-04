package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestConstraintRecordEncode(t *testing.T) {
	t.Run("制約レコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		cr := NewConstraintRecord(page.FileId(1), "id", "PRIMARY", page.FileId(0), "")

		// WHEN
		record := cr.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("主キー制約をエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewConstraintRecord(page.FileId(1), "id", "PRIMARY", page.FileId(0), "")

		// WHEN
		record := original.encode()
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, original.FileId, decoded.FileId)
		assert.Equal(t, original.ColName, decoded.ColName)
		assert.Equal(t, original.ConstraintName, decoded.ConstraintName)
		assert.Equal(t, original.RefTableFileId, decoded.RefTableFileId)
		assert.Equal(t, original.RefColName, decoded.RefColName)
	})

	t.Run("外部キー制約をエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewConstraintRecord(
			page.FileId(2),
			"user_id",
			"fk_orders_users",
			page.FileId(1),
			"id",
		)

		// WHEN
		record := original.encode()
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, page.FileId(2), decoded.FileId)
		assert.Equal(t, "user_id", decoded.ColName)
		assert.Equal(t, "fk_orders_users", decoded.ConstraintName)
		assert.Equal(t, page.FileId(1), decoded.RefTableFileId)
		assert.Equal(t, "id", decoded.RefColName)
	})

	t.Run("ユニークキー制約をエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewConstraintRecord(page.FileId(1), "email", "idx_email", page.FileId(0), "")

		// WHEN
		record := original.encode()
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, "email", decoded.ColName)
		assert.Equal(t, "idx_email", decoded.ConstraintName)
		assert.Equal(t, page.FileId(0), decoded.RefTableFileId)
		assert.Equal(t, "", decoded.RefColName)
	})

	t.Run("FileId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewConstraintRecord(page.FileId(0), "col", "pk", page.FileId(0), "")

		// WHEN
		record := original.encode()
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, page.FileId(0), decoded.FileId)
		assert.Equal(t, page.FileId(0), decoded.RefTableFileId)
	})
}

func TestDecodeConstraintRecord(t *testing.T) {
	t.Run("エンコード済みレコードから FileId とカラム名と制約名を復元できる", func(t *testing.T) {
		// GIVEN
		cr := NewConstraintRecord(page.FileId(42), "name", "uq_name", page.FileId(0), "")
		record := cr.encode()

		// WHEN
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, page.FileId(42), decoded.FileId)
		assert.Equal(t, "name", decoded.ColName)
		assert.Equal(t, "uq_name", decoded.ConstraintName)
	})

	t.Run("エンコード済みレコードから参照先テーブルとカラムを復元できる", func(t *testing.T) {
		// GIVEN
		cr := NewConstraintRecord(page.FileId(3), "dept_id", "fk_dept", page.FileId(5), "id")
		record := cr.encode()

		// WHEN
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, page.FileId(5), decoded.RefTableFileId)
		assert.Equal(t, "id", decoded.RefColName)
	})
}

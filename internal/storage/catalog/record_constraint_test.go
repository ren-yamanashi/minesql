package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestConstraintRecordEncode(t *testing.T) {
	t.Run("制約レコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		cr := ConstraintRecord{FileId: page.FileId(1), ColName: "id", ConstraintName: "PRIMARY", RefTableFileId: page.FileId(0), RefColName: ""}

		// WHEN
		record := cr.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("主キー制約をエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := ConstraintRecord{FileId: page.FileId(1), ColName: "id", ConstraintName: "PRIMARY", RefTableFileId: page.FileId(0), RefColName: ""}

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
		original := ConstraintRecord{FileId: page.FileId(2), ColName: "user_id", ConstraintName: "fk_orders_users", RefTableFileId: page.FileId(1), RefColName: "id"}

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
		original := ConstraintRecord{FileId: page.FileId(1), ColName: "email", ConstraintName: "idx_email", RefTableFileId: page.FileId(0), RefColName: ""}

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
		original := ConstraintRecord{FileId: page.FileId(0), ColName: "col", ConstraintName: "pk", RefTableFileId: page.FileId(0), RefColName: ""}

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
		cr := ConstraintRecord{FileId: page.FileId(42), ColName: "name", ConstraintName: "uq_name", RefTableFileId: page.FileId(0), RefColName: ""}
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
		cr := ConstraintRecord{FileId: page.FileId(3), ColName: "dept_id", ConstraintName: "fk_dept", RefTableFileId: page.FileId(5), RefColName: "id"}
		record := cr.encode()

		// WHEN
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, page.FileId(5), decoded.RefTableFileId)
		assert.Equal(t, "id", decoded.RefColName)
	})
}

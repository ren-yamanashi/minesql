package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestConstraintRecordFileId(t *testing.T) {
	t.Run("コンストラクタで指定した FileId を返す", func(t *testing.T) {
		// GIVEN
		cr := NewConstraintRecord(page.FileId(1), "id", "PRIMARY", page.FileId(0), "")

		// WHEN
		got := cr.FileId()

		// THEN
		assert.Equal(t, page.FileId(1), got)
	})
}

func TestConstraintRecordColumnName(t *testing.T) {
	t.Run("コンストラクタで指定したカラム名を返す", func(t *testing.T) {
		// GIVEN
		cr := NewConstraintRecord(page.FileId(1), "user_id", "fk_user", page.FileId(2), "id")

		// WHEN
		got := cr.ColumnName()

		// THEN
		assert.Equal(t, "user_id", got)
	})
}

func TestConstraintRecordConstraintName(t *testing.T) {
	t.Run("コンストラクタで指定した制約名を返す", func(t *testing.T) {
		// GIVEN
		cr := NewConstraintRecord(page.FileId(1), "id", "PRIMARY", page.FileId(0), "")

		// WHEN
		got := cr.ConstraintName()

		// THEN
		assert.Equal(t, "PRIMARY", got)
	})
}

func TestConstraintRecordReferenceTableFileId(t *testing.T) {
	t.Run("コンストラクタで指定した参照先テーブルの FileId を返す", func(t *testing.T) {
		// GIVEN
		cr := NewConstraintRecord(page.FileId(2), "user_id", "fk_user", page.FileId(5), "id")

		// WHEN
		got := cr.ReferenceTableFileId()

		// THEN
		assert.Equal(t, page.FileId(5), got)
	})
}

func TestConstraintRecordReferenceColumnName(t *testing.T) {
	t.Run("コンストラクタで指定した参照先カラム名を返す", func(t *testing.T) {
		// GIVEN
		cr := NewConstraintRecord(page.FileId(2), "user_id", "fk_user", page.FileId(5), "id")

		// WHEN
		got := cr.ReferenceColumnName()

		// THEN
		assert.Equal(t, "id", got)
	})
}

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
		assert.Equal(t, original.FileId(), decoded.FileId())
		assert.Equal(t, original.ColumnName(), decoded.ColumnName())
		assert.Equal(t, original.ConstraintName(), decoded.ConstraintName())
		assert.Equal(t, original.ReferenceTableFileId(), decoded.ReferenceTableFileId())
		assert.Equal(t, original.ReferenceColumnName(), decoded.ReferenceColumnName())
	})

	t.Run("外部キー制約をエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewConstraintRecord(page.FileId(2), "user_id", "fk_orders_users", page.FileId(1), "id")

		// WHEN
		record := original.encode()
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, page.FileId(2), decoded.FileId())
		assert.Equal(t, "user_id", decoded.ColumnName())
		assert.Equal(t, "fk_orders_users", decoded.ConstraintName())
		assert.Equal(t, page.FileId(1), decoded.ReferenceTableFileId())
		assert.Equal(t, "id", decoded.ReferenceColumnName())
	})

	t.Run("ユニークキー制約をエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewConstraintRecord(page.FileId(1), "email", "idx_email", page.FileId(0), "")

		// WHEN
		record := original.encode()
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, "email", decoded.ColumnName())
		assert.Equal(t, "idx_email", decoded.ConstraintName())
		assert.Equal(t, page.FileId(0), decoded.ReferenceTableFileId())
		assert.Equal(t, "", decoded.ReferenceColumnName())
	})

	t.Run("FileId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewConstraintRecord(page.FileId(0), "col", "pk", page.FileId(0), "")

		// WHEN
		record := original.encode()
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, page.FileId(0), decoded.FileId())
		assert.Equal(t, page.FileId(0), decoded.ReferenceTableFileId())
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
		assert.Equal(t, page.FileId(42), decoded.FileId())
		assert.Equal(t, "name", decoded.ColumnName())
		assert.Equal(t, "uq_name", decoded.ConstraintName())
	})

	t.Run("エンコード済みレコードから参照先テーブルとカラムを復元できる", func(t *testing.T) {
		// GIVEN
		cr := NewConstraintRecord(page.FileId(3), "dept_id", "fk_dept", page.FileId(5), "id")
		record := cr.encode()

		// WHEN
		decoded := decodeConstraintRecord(record)

		// THEN
		assert.Equal(t, page.FileId(5), decoded.ReferenceTableFileId())
		assert.Equal(t, "id", decoded.ReferenceColumnName())
	})
}

package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestColumnMetaRecordFileId(t *testing.T) {
	t.Run("コンストラクタで指定した FileId を返す", func(t *testing.T) {
		// GIVEN
		cr := NewColumnMetaRecord(page.FileId(1), "name", 0)

		// WHEN
		got := cr.FileId()

		// THEN
		assert.Equal(t, page.FileId(1), got)
	})
}

func TestColumnMetaRecordName(t *testing.T) {
	t.Run("コンストラクタで指定したカラム名を返す", func(t *testing.T) {
		// GIVEN
		cr := NewColumnMetaRecord(page.FileId(1), "email", 2)

		// WHEN
		got := cr.Name()

		// THEN
		assert.Equal(t, "email", got)
	})
}

func TestColumnMetaRecordPosition(t *testing.T) {
	t.Run("コンストラクタで指定したカラム位置を返す", func(t *testing.T) {
		// GIVEN
		cr := NewColumnMetaRecord(page.FileId(1), "name", 5)

		// WHEN
		got := cr.Position()

		// THEN
		assert.Equal(t, 5, got)
	})
}

func TestColumnMetaRecordEncode(t *testing.T) {
	t.Run("カラムレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		cr := NewColumnMetaRecord(page.FileId(1), "name", 0)

		// WHEN
		record := cr.Encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("エンコードした結果をデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := NewColumnMetaRecord(page.FileId(1), "email", 2)

		// WHEN
		record := original.Encode()
		decoded, err := DecodeColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, original.FileId(), decoded.FileId())
		assert.Equal(t, original.Name(), decoded.Name())
		assert.Equal(t, original.Position(), decoded.Position())
	})

	t.Run("カラム位置が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewColumnMetaRecord(page.FileId(2), "id", 0)

		// WHEN
		record := original.Encode()
		decoded, err := DecodeColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, page.FileId(2), decoded.FileId())
		assert.Equal(t, "id", decoded.Name())
		assert.Equal(t, 0, decoded.Position())
	})

	t.Run("FileId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewColumnMetaRecord(page.FileId(0), "col", 5)

		// WHEN
		record := original.Encode()
		decoded, err := DecodeColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, page.FileId(0), decoded.FileId())
		assert.Equal(t, "col", decoded.Name())
		assert.Equal(t, 5, decoded.Position())
	})

	t.Run("長いカラム名でも正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewColumnMetaRecord(page.FileId(3), "very_long_column_name_for_testing", 10)

		// WHEN
		record := original.Encode()
		decoded, err := DecodeColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, "very_long_column_name_for_testing", decoded.Name())
	})
}

func TestDecodeColumnMetaRecord(t *testing.T) {
	t.Run("エンコード済みレコードから FileId とカラム名を復元できる", func(t *testing.T) {
		// GIVEN
		cr := NewColumnMetaRecord(page.FileId(42), "age", 3)
		record := cr.Encode()

		// WHEN
		decoded, err := DecodeColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, page.FileId(42), decoded.FileId())
		assert.Equal(t, "age", decoded.Name())
	})

	t.Run("エンコード済みレコードからカラム位置を復元できる", func(t *testing.T) {
		// GIVEN
		cr := NewColumnMetaRecord(page.FileId(1), "status", 7)
		record := cr.Encode()

		// WHEN
		decoded, err := DecodeColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 7, decoded.Position())
	})
}

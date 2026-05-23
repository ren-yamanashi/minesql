package catalog

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestTableRecordName(t *testing.T) {
	t.Run("コンストラクタで指定した名前を返す", func(t *testing.T) {
		// GIVEN
		tr := NewTableRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3)

		// WHEN
		got := tr.Name()

		// THEN
		assert.Equal(t, "users", got)
	})
}

func TestTableRecordMetaPageId(t *testing.T) {
	t.Run("コンストラクタで指定したメタページ ID を返す", func(t *testing.T) {
		// GIVEN
		metaPageId := page.NewId(page.FileId(1), page.PageNumber(0))
		tr := NewTableRecord("users", metaPageId, 3)

		// WHEN
		got := tr.MetaPageId()

		// THEN
		assert.Equal(t, metaPageId, got)
	})
}

func TestTableRecordColumnCount(t *testing.T) {
	t.Run("コンストラクタで指定したカラム数を返す", func(t *testing.T) {
		// GIVEN
		tr := NewTableRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3)

		// WHEN
		got := tr.ColumnCount()

		// THEN
		assert.Equal(t, 3, got)
	})
}

func TestTableRecordEncode(t *testing.T) {
	t.Run("テーブルレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		tr := NewTableRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3)

		// WHEN
		record := tr.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("エンコードした結果をデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := NewTableRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, original.MetaPageId(), decoded.MetaPageId())
		assert.Equal(t, original.Name(), decoded.Name())
		assert.Equal(t, original.ColumnCount(), decoded.ColumnCount())
	})

	t.Run("カラム数が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewTableRecord("empty_table", page.NewId(page.FileId(2), page.PageNumber(0)), 0)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, original.MetaPageId(), decoded.MetaPageId())
		assert.Equal(t, original.Name(), decoded.Name())
		assert.Equal(t, original.ColumnCount(), decoded.ColumnCount())
	})

	t.Run("長いテーブル名でも正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewTableRecord("very_long_table_name_for_testing", page.NewId(page.FileId(3), page.PageNumber(0)), 10)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, original.Name(), decoded.Name())
	})

	t.Run("MetaPageId のページ番号が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewTableRecord("t", page.NewId(page.FileId(0), page.PageNumber(0)), 1)

		// WHEN
		record := original.encode()
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, page.NewId(page.FileId(0), page.PageNumber(0)), decoded.MetaPageId())
		assert.Equal(t, "t", decoded.Name())
		assert.Equal(t, 1, decoded.ColumnCount())
	})
}

func TestDecodeTableRecord(t *testing.T) {
	t.Run("エンコード済みレコードから MetaPageId を復元できる", func(t *testing.T) {
		// GIVEN
		tr := NewTableRecord("orders", page.NewId(page.FileId(42), page.PageNumber(0)), 5)
		record := tr.encode()

		// WHEN
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, page.NewId(page.FileId(42), page.PageNumber(0)), decoded.MetaPageId())
	})

	t.Run("エンコード済みレコードからテーブル名とカラム数を復元できる", func(t *testing.T) {
		// GIVEN
		tr := NewTableRecord("products", page.NewId(page.FileId(1), page.PageNumber(0)), 7)
		record := tr.encode()

		// WHEN
		decoded := decodeTableRecord(record)

		// THEN
		assert.Equal(t, "products", decoded.Name())
		assert.Equal(t, 7, decoded.ColumnCount())
	})
}

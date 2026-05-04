package catalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexKeyColRecordEncode(t *testing.T) {
	t.Run("インデックスキーカラムレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		kcr := NewIndexKeyColRecord(IndexId(1), "name", 0)

		// WHEN
		record := kcr.encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("エンコードした結果をデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := NewIndexKeyColRecord(IndexId(10), "email", 2)

		// WHEN
		record := original.encode()
		decoded := decodeIndexKeyColRecord(record)

		// THEN
		assert.Equal(t, original.IndexId, decoded.IndexId)
		assert.Equal(t, original.Name, decoded.Name)
		assert.Equal(t, original.Pos, decoded.Pos)
	})

	t.Run("カラム位置が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewIndexKeyColRecord(IndexId(5), "id", 0)

		// WHEN
		record := original.encode()
		decoded := decodeIndexKeyColRecord(record)

		// THEN
		assert.Equal(t, IndexId(5), decoded.IndexId)
		assert.Equal(t, "id", decoded.Name)
		assert.Equal(t, 0, decoded.Pos)
	})

	t.Run("IndexId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewIndexKeyColRecord(IndexId(0), "col", 3)

		// WHEN
		record := original.encode()
		decoded := decodeIndexKeyColRecord(record)

		// THEN
		assert.Equal(t, IndexId(0), decoded.IndexId)
		assert.Equal(t, "col", decoded.Name)
		assert.Equal(t, 3, decoded.Pos)
	})
}

func TestDecodeIndexKeyColRecord(t *testing.T) {
	t.Run("エンコード済みレコードからインデックス ID とカラム名を復元できる", func(t *testing.T) {
		// GIVEN
		kcr := NewIndexKeyColRecord(IndexId(42), "age", 5)
		record := kcr.encode()

		// WHEN
		decoded := decodeIndexKeyColRecord(record)

		// THEN
		assert.Equal(t, IndexId(42), decoded.IndexId)
		assert.Equal(t, "age", decoded.Name)
	})

	t.Run("エンコード済みレコードからカラム位��を復元できる", func(t *testing.T) {
		// GIVEN
		kcr := NewIndexKeyColRecord(IndexId(1), "status", 7)
		record := kcr.encode()

		// WHEN
		decoded := decodeIndexKeyColRecord(record)

		// THEN
		assert.Equal(t, 7, decoded.Pos)
	})
}

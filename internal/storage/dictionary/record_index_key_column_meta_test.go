package dictionary

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexKeyColumnMetaRecordIndexId(t *testing.T) {
	t.Run("コンストラクタで指定したインデックス ID を返す", func(t *testing.T) {
		// GIVEN
		kcr := NewIndexKeyColumnMetaRecord(IndexId(1), "name", 0)

		// WHEN
		got := kcr.IndexId()

		// THEN
		assert.Equal(t, IndexId(1), got)
	})
}

func TestIndexKeyColumnMetaRecordName(t *testing.T) {
	t.Run("コンストラクタで指定したカラム名を返す", func(t *testing.T) {
		// GIVEN
		kcr := NewIndexKeyColumnMetaRecord(IndexId(1), "name", 0)

		// WHEN
		got := kcr.Name()

		// THEN
		assert.Equal(t, "name", got)
	})
}

func TestIndexKeyColumnMetaRecordPosition(t *testing.T) {
	t.Run("コンストラクタで指定したカラム位置を返す", func(t *testing.T) {
		// GIVEN
		kcr := NewIndexKeyColumnMetaRecord(IndexId(1), "name", 3)

		// WHEN
		got := kcr.Position()

		// THEN
		assert.Equal(t, 3, got)
	})
}

func TestIndexKeyColumnMetaRecordEncode(t *testing.T) {
	t.Run("インデックスキーカラムレコードをエンコードできる", func(t *testing.T) {
		// GIVEN
		kcr := NewIndexKeyColumnMetaRecord(IndexId(1), "name", 0)

		// WHEN
		record := kcr.Encode()

		// THEN
		assert.NotNil(t, record.Key())
		assert.NotNil(t, record.NonKey())
		assert.Nil(t, record.Header())
	})

	t.Run("エンコードした結果をデコードすると元のデータに戻る", func(t *testing.T) {
		// GIVEN
		original := NewIndexKeyColumnMetaRecord(IndexId(10), "email", 2)

		// WHEN
		record := original.Encode()
		decoded, err := DecodeIndexKeyColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, original.IndexId(), decoded.IndexId())
		assert.Equal(t, original.Name(), decoded.Name())
		assert.Equal(t, original.Position(), decoded.Position())
	})

	t.Run("カラム位置が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewIndexKeyColumnMetaRecord(IndexId(5), "id", 0)

		// WHEN
		record := original.Encode()
		decoded, err := DecodeIndexKeyColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, IndexId(5), decoded.IndexId())
		assert.Equal(t, "id", decoded.Name())
		assert.Equal(t, 0, decoded.Position())
	})

	t.Run("IndexId が 0 の場合も正しくエンコード・デコードできる", func(t *testing.T) {
		// GIVEN
		original := NewIndexKeyColumnMetaRecord(IndexId(0), "col", 3)

		// WHEN
		record := original.Encode()
		decoded, err := DecodeIndexKeyColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, IndexId(0), decoded.IndexId())
		assert.Equal(t, "col", decoded.Name())
		assert.Equal(t, 3, decoded.Position())
	})
}

func TestDecodeIndexKeyColumnMetaRecord(t *testing.T) {
	t.Run("エンコード済みレコードからインデックス ID とカラム名を復元できる", func(t *testing.T) {
		// GIVEN
		kcr := NewIndexKeyColumnMetaRecord(IndexId(42), "age", 5)
		record := kcr.Encode()

		// WHEN
		decoded, err := DecodeIndexKeyColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, IndexId(42), decoded.IndexId())
		assert.Equal(t, "age", decoded.Name())
	})

	t.Run("エンコード済みレコードからカラム位置を復元できる", func(t *testing.T) {
		// GIVEN
		kcr := NewIndexKeyColumnMetaRecord(IndexId(1), "status", 7)
		record := kcr.Encode()

		// WHEN
		decoded, err := DecodeIndexKeyColumnMetaRecord(record)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 7, decoded.Position())
	})
}

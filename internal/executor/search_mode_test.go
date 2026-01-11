package executor

import (
	"bytes"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeForRecordSearchModeStart(t *testing.T) {
	t.Run("RecordSearchModeStart が btree.SearchModeStart にエンコードされる", func(t *testing.T) {
		// GIVEN
		searchMode := RecordSearchModeStart{}

		// WHEN
		encoded := searchMode.Encode()

		// THEN
		_, ok := encoded.(btree.SearchModeStart)
		assert.True(t, ok, "エンコード結果が btree.SearchModeStart 型であること")
	})
}

func TestEncodeForRecordSearchModeKey(t *testing.T) {
	t.Run("RecordSearchModeKey が btree.SearchModeKey にエンコードされる", func(t *testing.T) {
		// GIVEN
		key := [][]byte{[]byte("test")}
		searchMode := RecordSearchModeKey{Key: key}

		// WHEN
		encoded := searchMode.Encode()

		// THEN
		result, ok := encoded.(btree.SearchModeKey)
		assert.True(t, ok, "エンコード結果が btree.SearchModeKey 型であること")
		assert.NotNil(t, result.Key)
	})

	t.Run("キーが正しくエンコードされる", func(t *testing.T) {
		// GIVEN
		key := [][]byte{[]byte("foo"), []byte("bar")}
		searchMode := RecordSearchModeKey{Key: key}

		// 期待値を手動でエンコード
		var expectedKey []byte
		table.Encode(key, &expectedKey)

		// WHEN
		encoded := searchMode.Encode()
		result := encoded.(btree.SearchModeKey)

		// THEN
		assert.True(t, bytes.Equal(result.Key, expectedKey), "エンコードされたキーが table.Encode の結果と一致すること")
	})

	t.Run("空のキーがエンコードされる", func(t *testing.T) {
		// GIVEN
		key := [][]byte{}
		searchMode := RecordSearchModeKey{Key: key}

		var expectedKey []byte
		table.Encode(key, &expectedKey)

		// WHEN
		encoded := searchMode.Encode()

		// THEN
		result, ok := encoded.(btree.SearchModeKey)
		assert.True(t, ok)
		assert.Equal(t, expectedKey, result.Key, "空のキーも正しくエンコードされること")
	})

	t.Run("単一のキーがエンコードされる", func(t *testing.T) {
		// GIVEN
		key := [][]byte{[]byte("single")}
		searchMode := RecordSearchModeKey{Key: key}

		var expectedKey []byte
		table.Encode(key, &expectedKey)

		// WHEN
		encoded := searchMode.Encode()
		result := encoded.(btree.SearchModeKey)

		// THEN
		assert.True(t, bytes.Equal(result.Key, expectedKey))
	})
}

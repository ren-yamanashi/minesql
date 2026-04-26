package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/encode"
	"github.com/stretchr/testify/assert"
)

func TestRecordSearchModeStart(t *testing.T) {
	t.Run("encode が btree.SearchModeStart を返す", func(t *testing.T) {
		// GIVEN
		mode := RecordSearchModeStart{}

		// WHEN
		encoded := mode.encode()

		// THEN
		assert.IsType(t, btree.SearchModeStart{}, encoded)
	})
}

func TestRecordSearchModeKey(t *testing.T) {
	t.Run("encode が memcomparable エンコード済みキーを持つ btree.SearchModeKey を返す", func(t *testing.T) {
		// GIVEN
		mode := RecordSearchModeKey{Key: [][]byte{[]byte("hello")}}

		// WHEN
		encoded := mode.encode()

		// THEN
		smKey, ok := encoded.(btree.SearchModeKey)
		assert.True(t, ok)

		// 期待されるエンコード結果と一致する
		var expected []byte
		encode.Encode([][]byte{[]byte("hello")}, &expected)
		assert.Equal(t, expected, smKey.Key)
	})

	t.Run("複数カラムのキーが正しくエンコードされる", func(t *testing.T) {
		// GIVEN
		mode := RecordSearchModeKey{Key: [][]byte{[]byte("a"), []byte("b")}}

		// WHEN
		encoded := mode.encode()

		// THEN
		smKey, ok := encoded.(btree.SearchModeKey)
		assert.True(t, ok)

		var expected []byte
		encode.Encode([][]byte{[]byte("a"), []byte("b")}, &expected)
		assert.Equal(t, expected, smKey.Key)
	})

	t.Run("空のキーでもパニックせずにエンコードできる", func(t *testing.T) {
		// GIVEN
		mode := RecordSearchModeKey{Key: [][]byte{}}

		// WHEN
		encoded := mode.encode()

		// THEN
		smKey, ok := encoded.(btree.SearchModeKey)
		assert.True(t, ok)

		var expected []byte
		encode.Encode([][]byte{}, &expected)
		assert.Equal(t, expected, smKey.Key)
	})

	t.Run("RecordSearchMode インターフェースを満たす", func(t *testing.T) {
		// GIVEN & WHEN & THEN
		var _ RecordSearchMode = RecordSearchModeStart{}
		var _ RecordSearchMode = RecordSearchModeKey{}
	})
}

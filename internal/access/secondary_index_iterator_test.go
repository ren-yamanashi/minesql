package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSecondaryIndexIterator(t *testing.T) {
	t.Run("インデックス経由でテーブルレコードをデコード済みで取得できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "idx_iter_test.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("idx_last_name", "last_name", indexMetaPageId, 2)

		table := NewTableAccessMethod("users", metaPageId, 1, []*UniqueIndexAccessMethod{uniqueIndex})
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
		assert.NoError(t, err)

		// WHEN: インデックスを先頭から検索
		iter, err := uniqueIndex.Search(bp, &table, RecordSearchModeStart{})
		assert.NoError(t, err)

		var results []*SecondaryIndexSearchResult
		for {
			result, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			results = append(results, result)
		}

		// THEN: セカンダリキー (last_name) の昇順で返される
		assert.Equal(t, 3, len(results))

		// "Doe" → record "a"
		assert.Equal(t, [][]byte{[]byte("Doe")}, results[0].SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}, results[0].Record)

		// "Johnson" → record "c"
		assert.Equal(t, [][]byte{[]byte("Johnson")}, results[1].SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")}, results[1].Record)

		// "Smith" → record "b"
		assert.Equal(t, [][]byte{[]byte("Smith")}, results[2].SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")}, results[2].Record)
	})

	t.Run("指定キーからインデックス検索できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "idx_iter_test.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("idx_last_name", "last_name", indexMetaPageId, 2)

		table := NewTableAccessMethod("users", metaPageId, 1, []*UniqueIndexAccessMethod{uniqueIndex})
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: キー "Smith" で検索
		iter, err := uniqueIndex.Search(bp, &table, RecordSearchModeKey{Key: [][]byte{[]byte("Smith")}})
		assert.NoError(t, err)

		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("Smith")}, result.SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")}, result.Record)
	})

	t.Run("空のインデックスでは ok が false を返す", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "idx_iter_test.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("idx_col", "col", indexMetaPageId, 0)

		table := NewTableAccessMethod("test", metaPageId, 1, []*UniqueIndexAccessMethod{uniqueIndex})
		err = table.Create(bp)
		assert.NoError(t, err)

		// WHEN
		iter, err := uniqueIndex.Search(bp, &table, RecordSearchModeStart{})
		assert.NoError(t, err)

		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, result)
	})

	t.Run("ソフトデリート済みのインデックスエントリはスキップされる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "idx_iter_test.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("idx_last_name", "last_name", indexMetaPageId, 2)

		table := NewTableAccessMethod("users", metaPageId, 1, []*UniqueIndexAccessMethod{uniqueIndex})
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
		assert.NoError(t, err)

		// "Doe" を持つ行をソフトデリート
		err = table.SoftDelete(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)

		// WHEN: インデックスを先頭から検索
		iter, err := uniqueIndex.Search(bp, &table, RecordSearchModeStart{})
		assert.NoError(t, err)

		var results []*SecondaryIndexSearchResult
		for {
			result, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			results = append(results, result)
		}

		// THEN: ソフトデリート済みの "Doe" はスキップされ、2 件のみ返される
		assert.Equal(t, 2, len(results))
		assert.Equal(t, [][]byte{[]byte("Johnson")}, results[0].SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")}, results[0].Record)
		assert.Equal(t, [][]byte{[]byte("Smith")}, results[1].SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")}, results[1].Record)
	})

	t.Run("更新後のインデックスから正しいレコードが返される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "idx_iter_test.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndexAccessMethod("idx_last_name", "last_name", indexMetaPageId, 2)

		table := NewTableAccessMethod("users", metaPageId, 1, []*UniqueIndexAccessMethod{uniqueIndex})
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)

		// テーブルとインデックスを更新
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Williams")}
		err = table.UpdateInplace(bp, newRecord)
		assert.NoError(t, err)
		oldRec := NewRecord(oldRecord, table.PrimaryKeyCount)
		encodedPK := oldRec.EncodeKey()
		for _, ui := range table.UniqueIndexes {
			err = ui.Delete(bp, encodedPK, oldRecord)
			assert.NoError(t, err)
			err = ui.Insert(bp, encodedPK, newRecord)
			assert.NoError(t, err)
		}

		// WHEN: 更新後のインデックスを検索
		iter, err := uniqueIndex.Search(bp, &table, RecordSearchModeKey{Key: [][]byte{[]byte("Williams")}})
		assert.NoError(t, err)

		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("Williams")}, result.SecondaryKey)
		assert.Equal(t, [][]byte{[]byte("a"), []byte("John"), []byte("Williams")}, result.Record)
	})
}

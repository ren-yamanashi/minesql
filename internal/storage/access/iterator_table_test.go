package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIterator(t *testing.T) {
	t.Run("先頭から全レコードをデコード済みで取得できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "iter_test.db")
		table := NewTable("test", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, 0, nil, RecordSearchModeStart{})
		assert.NoError(t, err)

		var records [][]byte
		for {
			record, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			records = append(records, record...)
		}

		// THEN: 3 レコード x 3 カラム = 9 要素
		assert.Equal(t, 9, len(records))
		assert.Equal(t, "a", string(records[0]))
		assert.Equal(t, "John", string(records[1]))
		assert.Equal(t, "Doe", string(records[2]))
		assert.Equal(t, "b", string(records[3]))
		assert.Equal(t, "Alice", string(records[4]))
		assert.Equal(t, "Smith", string(records[5]))
		assert.Equal(t, "c", string(records[6]))
		assert.Equal(t, "Bob", string(records[7]))
		assert.Equal(t, "Johnson", string(records[8]))
	})

	t.Run("指定キーからレコードを取得できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "iter_test.db")
		table := NewTable("test", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("c"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN: キー "b" から検索
		iter, err := table.Search(bp, 0, nil, RecordSearchModeKey{Key: [][]byte{[]byte("b")}})
		assert.NoError(t, err)

		record, ok, err := iter.Next()

		// THEN: "b" のレコードが返る
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("b"), []byte("Alice")}, record)
	})

	t.Run("空のテーブルでは ok が false を返す", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "iter_test.db")
		table := NewTable("test", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, 0, nil, RecordSearchModeStart{})
		assert.NoError(t, err)

		record, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, record)
	})

	t.Run("DeleteMark が設定されたレコードはスキップされる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "iter_test.db")
		table := NewTable("test", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("c"), []byte("Charlie")})
		assert.NoError(t, err)

		// "b" をソフトデリート
		err = table.SoftDelete(bp, 0, nil, [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, 0, nil, RecordSearchModeStart{})
		assert.NoError(t, err)

		var records [][][]byte
		for {
			record, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
			records = append(records, record)
		}

		// THEN: ソフトデリートされた "b" はスキップされ、2 件のみ
		assert.Equal(t, 2, len(records))
		assert.Equal(t, "a", string(records[0][0]))
		assert.Equal(t, "c", string(records[1][0]))
	})

	t.Run("全レコードがソフトデリートされている場合、ok が false を返す", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "iter_test.db")
		table := NewTable("test", metaPageId, 1, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)

		err = table.SoftDelete(bp, 0, nil, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.SoftDelete(bp, 0, nil, [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, 0, nil, RecordSearchModeStart{})
		assert.NoError(t, err)

		record, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Nil(t, record)
	})

	t.Run("複合プライマリキーのレコードをデコードできる", func(t *testing.T) {
		// GIVEN: PrimaryKeyCount = 2
		bp, metaPageId, _ := InitDisk(t, "iter_test.db")
		table := NewTable("test", metaPageId, 2, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("1"), []byte("value1")})
		assert.NoError(t, err)
		err = table.Insert(bp, [][]byte{[]byte("a"), []byte("2"), []byte("value2")})
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, 0, nil, RecordSearchModeStart{})
		assert.NoError(t, err)

		record1, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)

		record2, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)

		// THEN: プライマリキー 2 カラム + 値 1 カラム
		assert.Equal(t, [][]byte{[]byte("a"), []byte("1"), []byte("value1")}, record1)
		assert.Equal(t, [][]byte{[]byte("a"), []byte("2"), []byte("value2")}, record2)
	})
}

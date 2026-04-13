package access

import (
	"minesql/internal/storage/lock"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableIterator(t *testing.T) {
	t.Run("先頭から全レコードをデコード済みで取得できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "iter_test.db")
		table := NewTable("test", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, allVisibleReadView(), nilVersionReader(), RecordSearchModeStart{})
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
		table := NewTable("test", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("c"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN: キー "b" から検索
		iter, err := table.Search(bp, allVisibleReadView(), nilVersionReader(), RecordSearchModeKey{Key: [][]byte{[]byte("b")}})
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
		table := NewTable("test", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, allVisibleReadView(), nilVersionReader(), RecordSearchModeStart{})
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
		table := NewTable("test", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("c"), []byte("Charlie")})
		assert.NoError(t, err)

		// "b" をソフトデリート
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, allVisibleReadView(), nilVersionReader(), RecordSearchModeStart{})
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
		table := NewTable("test", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)

		err = table.SoftDelete(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, allVisibleReadView(), nilVersionReader(), RecordSearchModeStart{})
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
		table := NewTable("test", metaPageId, 2, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("1"), []byte("value1")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("2"), []byte("value2")})
		assert.NoError(t, err)

		// WHEN
		iter, err := table.Search(bp, allVisibleReadView(), nilVersionReader(), RecordSearchModeStart{})
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

	t.Run("不可視なトランザクションのレコードはスキップされる", func(t *testing.T) {
		// GIVEN: T1 が INSERT、T2 が INSERT。T2 はアクティブ (不可視)
		bp, metaPageId, _ := InitDisk(t, "iter_test.db")
		table := NewTable("test", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 1, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, 2, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN: T3 の ReadView で T2 がアクティブ (不可視)
		rv := NewReadView(3, []TrxId{2}, 4) // mIds=[2]
		iter, err := table.Search(bp, rv, NewVersionReader(nil), RecordSearchModeStart{})
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

		// THEN: T1 の "a" のみ可視。T2 の "b" はスキップされる
		assert.Equal(t, 1, len(records))
		assert.Equal(t, "a", string(records[0][0]))
		assert.Equal(t, "Alice", string(records[0][1]))
	})

	t.Run("自分のトランザクションのレコードは可視", func(t *testing.T) {
		// GIVEN: T1 が INSERT
		bp, metaPageId, _ := InitDisk(t, "iter_test.db")
		table := NewTable("test", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 1, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN: T1 自身の ReadView
		rv := NewReadView(1, nil, 2)
		iter, err := table.Search(bp, rv, NewVersionReader(nil), RecordSearchModeStart{})
		assert.NoError(t, err)

		record, ok, err := iter.Next()

		// THEN: 自分の INSERT は可視
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "a", string(record[0]))
	})
}

// allVisibleReadView は全レコードが可視な ReadView を返す (テスト用)
func allVisibleReadView() *ReadView {
	return NewReadView(0, nil, ^uint64(0))
}

// nilVersionReader は undo チェーンを辿らない VersionReader を返す (テスト用)
func nilVersionReader() *VersionReader {
	return NewVersionReader(nil)
}

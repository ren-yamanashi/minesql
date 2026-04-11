package access

import (
	"fmt"
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/encode"
	"minesql/internal/storage/file"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateAndInsert(t *testing.T) {
	t.Run("テーブルの作成ができ、そのテーブルに値が挿入できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", indexMetaPageId, 2, 1)

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex}, nil, nil)

		// WHEN: テーブルを作成
		err = table.Create(bp)
		assert.NoError(t, err)

		// WHEN: 行を挿入
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
		assert.NoError(t, err)

		// THEN: 挿入したデータが B+Tree に存在する
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		expectedRecords := []struct {
			key   [][]byte
			value [][]byte
		}{
			{[][]byte{[]byte("a")}, [][]byte{[]byte("John"), []byte("Doe")}},
			{[][]byte{[]byte("b")}, [][]byte{[]byte("Alice"), []byte("Smith")}},
			{[][]byte{[]byte("c")}, [][]byte{[]byte("Bob"), []byte("Johnson")}},
			{[][]byte{[]byte("d")}, [][]byte{[]byte("Eve"), []byte("Davis")}},
		}

		i := 0
		for {
			record, ok := iter.Get()
			if !ok {
				break
			}
			expected := expectedRecords[i]

			// エンコードされたキーと値をデコード
			var decodedKey [][]byte
			var decodedValue [][]byte
			keyBytes := record.KeyBytes()
			valueBytes := record.NonKeyBytes()
			encode.Decode(keyBytes, &decodedKey)
			encode.Decode(valueBytes, &decodedValue)

			assert.Equal(t, expected.key, decodedKey)
			assert.Equal(t, expected.value, decodedValue)

			i++
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}
		assert.Equal(t, len(expectedRecords), i)

		// THEN: ユニークインデックスにもデータが挿入されている
		uniqueIndexTree := btree.NewBTree(table.UniqueIndexes[0].MetaPageId)
		uniqueIndexIter, err := uniqueIndexTree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		// Key = concat(encodedSecondaryKey, encodedPK) で、NonKey は空
		// セカンダリキーでソートされ、同一セカンダリキー内では PK でソートされる
		expectedUniqueIndexRecords := []struct {
			secondaryKey string
			pk           string
		}{
			{"Davis", "d"},
			{"Doe", "a"},
			{"Johnson", "c"},
			{"Smith", "b"},
		}

		j := 0
		for {
			record, ok := uniqueIndexIter.Get()
			if !ok {
				break
			}
			expected := expectedUniqueIndexRecords[j]

			// Key をデコードしてセカンダリキーと PK を分離
			var keyColumns [][]byte
			encode.Decode(record.KeyBytes(), &keyColumns)

			assert.Equal(t, expected.secondaryKey, string(keyColumns[0]))
			assert.Equal(t, expected.pk, string(keyColumns[1]))
			// Header は DeleteMark=0
			assert.Equal(t, uint8(0), record.HeaderBytes()[0])
			// NonKey は nil (空)
			assert.Equal(t, 0, len(record.NonKeyBytes()))

			j++
			_, _, err := uniqueIndexIter.Next(bp)
			assert.NoError(t, err)
		}
		assert.Equal(t, len(expectedUniqueIndexRecords), j)
	})

	t.Run("テーブルとそのインデックスが同じディスクファイル (同じ FileId) に保存される", func(t *testing.T) {
		// GIVEN
		// 2つのインデックスを持つテーブルを作成
		bp, metaPageId, tmpdir := InitDisk(t, "users.db")

		indexMetaPageId1, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex1 := NewUniqueIndex("idx_first_name", "first_name", indexMetaPageId1, 1, 1)
		indexMetaPageId2, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex2 := NewUniqueIndex("idx_last_name", "last_name", indexMetaPageId2, 2, 1)

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex1, uniqueIndex2}, nil, nil)

		// WHEN
		err = table.Create(bp)
		assert.NoError(t, err)

		// THEN: テーブルとすべてのインデックスが同じ FileId を持つ
		assert.Equal(t, table.MetaPageId.FileId, uniqueIndex1.MetaPageId.FileId, "first_name インデックスはテーブルと同じ FileId を持つべき")
		assert.Equal(t, table.MetaPageId.FileId, uniqueIndex2.MetaPageId.FileId, "last_name インデックスはテーブルと同じ FileId を持つべき")

		// THEN: MetaPageId は異なる (各 B+Tree は別々のメタページを持つ)
		assert.NotEqual(t, table.MetaPageId, uniqueIndex1.MetaPageId, "テーブルとインデックスは異なる MetaPageId を持つべき")
		assert.NotEqual(t, table.MetaPageId, uniqueIndex2.MetaPageId, "テーブルとインデックスは異なる MetaPageId を持つべき")
		assert.NotEqual(t, uniqueIndex1.MetaPageId, uniqueIndex2.MetaPageId, "各インデックスは異なる MetaPageId を持つべき")

		// THEN: ディスクに作成されたファイルが1つだけである
		files, err := os.ReadDir(tmpdir)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(files), "ディスクファイルは1つだけ作成されるべき")
		assert.Equal(t, "users.db", files[0].Name(), "ファイル名はテーブル名.db であるべき")

		// THEN: ファイルパスが正しい
		expectedFilePath := filepath.Join(tmpdir, "users.db")
		_, err = os.Stat(expectedFilePath)
		assert.NoError(t, err, "users.db ファイルが存在するべき")
	})

	t.Run("active なレコードが存在する PK で Insert すると重複キーエラーが返る", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// WHEN: 同じ PK で Insert
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Jane")})

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("ソフトデリート済みの同一 PK を持つレコードが存在する場合、Insert で上書きされる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// ソフトデリート
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// WHEN: 同じ PK で再 Insert (値は異なる)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Jane")})

		// THEN: エラーなく挿入できる
		assert.NoError(t, err)

		// THEN: B+Tree を直接走査するとレコードは 1 件で、DeleteMark=0 に更新されている
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, uint8(0), record.HeaderBytes()[0]) // active

		var decodedKey [][]byte
		encode.Decode(record.KeyBytes(), &decodedKey)
		assert.Equal(t, "a", string(decodedKey[0]))

		var decodedValue [][]byte
		encode.Decode(record.NonKeyBytes(), &decodedValue)
		assert.Equal(t, "Jane", string(decodedValue[0])) // 新しい値で上書きされている

		// 2 件目は存在しない (物理的にレコードが増えていない)
		err = iter.Advance(bp)
		assert.NoError(t, err)
		_, ok = iter.Get()
		assert.False(t, ok)
	})

	t.Run("Insert で対象行に排他ロックが取得される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		lockMgr := lock.NewManager(200)

		// WHEN: trx1 が Insert で排他ロックを取得
		err = table.Insert(bp, 1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// THEN: trx2 が同じ行に排他ロックを取得しようとするとタイムアウト
		err = table.SoftDelete(bp, 2, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.ErrorIs(t, err, lock.ErrTimeout)
	})
}

func TestSoftDelete(t *testing.T) {
	t.Run("テーブルから行を削除でき、B+Tree とユニークインデックスの両方から削除される", func(t *testing.T) {
		// GIVEN: テーブルを作成しデータを挿入
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", indexMetaPageId, 2, 1)

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex}, nil, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		records := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		err = table.Insert(bp, 0, lock.NewManager(5000), records)
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
		assert.NoError(t, err)

		// WHEN: "a" の行を削除
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), records)

		// THEN: ソフトデリートされている (ClusteredIndexIterator で走査すると削除済みレコードはスキップされる)
		assert.NoError(t, err)
		recs := collectAllTablePairs(t, bp, &table)
		var keys []string
		for _, rec := range recs {
			keys = append(keys, string(rec.key[0]))
		}
		assert.Equal(t, []string{"b", "c"}, keys)

		// THEN: ユニークインデックスもソフトデリートされている
		// B+Tree を直接走査し、active なエントリのみ確認する
		indexTree := btree.NewBTree(table.UniqueIndexes[0].MetaPageId)
		indexIter, err := indexTree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		var indexKeys []string
		for {
			record, ok := indexIter.Get()
			if !ok {
				break
			}
			// ソフトデリート済みはスキップ
			if record.HeaderBytes()[0] != 1 {
				var decodedKey [][]byte
				encode.Decode(record.KeyBytes(), &decodedKey)
				indexKeys = append(indexKeys, string(decodedKey[0]))
			}
			_, _, err := indexIter.Next(bp)
			assert.NoError(t, err)
		}
		// "Doe" がソフトデリートされ、active なのは "Johnson", "Smith" のみ
		assert.Equal(t, []string{"Johnson", "Smith"}, indexKeys)
	})

	t.Run("Delete はレコードを物理削除せず DeleteMark を 1 にするソフトデリートである", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN: "a" をソフトデリート
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// THEN: B+Tree を直接走査すると 2 件存在し、"a" は DeleteMark=1
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		type entry struct {
			pk         string
			deleteMark uint8
		}
		var entries []entry
		for {
			record, ok := iter.Get()
			if !ok {
				break
			}
			var decodedKey [][]byte
			encode.Decode(record.KeyBytes(), &decodedKey)
			entries = append(entries, entry{
				pk:         string(decodedKey[0]),
				deleteMark: record.HeaderBytes()[0],
			})
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}

		assert.Equal(t, 2, len(entries))
		assert.Equal(t, "a", entries[0].pk)
		assert.Equal(t, uint8(1), entries[0].deleteMark) // ソフトデリート済み
		assert.Equal(t, "b", entries[1].pk)
		assert.Equal(t, uint8(0), entries[1].deleteMark) // active
	})

	t.Run("存在しないキーを削除するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// WHEN: 存在しないキーで削除
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), [][]byte{[]byte("z"), []byte("Unknown")})

		// THEN
		assert.Error(t, err)
	})

	t.Run("全行を削除した後にテーブルが空になる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		record1 := [][]byte{[]byte("a"), []byte("John")}
		record2 := [][]byte{[]byte("b"), []byte("Alice")}
		err = table.Insert(bp, 0, lock.NewManager(5000), record1)
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), record2)
		assert.NoError(t, err)

		// WHEN
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), record1)
		assert.NoError(t, err)
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), record2)
		assert.NoError(t, err)

		// THEN: ClusteredIndexIterator で走査すると active なレコードがない
		recs := collectAllTablePairs(t, bp, &table)
		assert.Equal(t, 0, len(recs))
	})

	t.Run("対象行に排他ロックが取得される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		lockMgr := lock.NewManager(200)

		// WHEN: trx1 が SoftDelete で排他ロックを取得
		err = table.SoftDelete(bp, 1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// THEN: trx2 が同じ行に排他ロックを取得しようとするとタイムアウト
		err = table.SoftDelete(bp, 2, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.ErrorIs(t, err, lock.ErrTimeout)
	})
}

func TestDelete(t *testing.T) {
	t.Run("テーブルから行を物理削除できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN: "a" を物理削除
		err = table.deleteRaw(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// THEN: B+Tree を直接走査すると 1 件のみ存在 (物理的に消えている)
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)
		var decodedKey [][]byte
		encode.Decode(record.KeyBytes(), &decodedKey)
		assert.Equal(t, "b", string(decodedKey[0]))

		err = iter.Advance(bp)
		assert.NoError(t, err)
		_, ok = iter.Get()
		assert.False(t, ok)
	})

	t.Run("物理削除するとユニークインデックスからも物理削除される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", indexMetaPageId, 2, 1)

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex}, nil, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: "a" を物理削除
		err = table.deleteRaw(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)

		// THEN: クラスタ化インデックスから物理削除されている
		recs := collectAllTablePairs(t, bp, &table)
		assert.Equal(t, 1, len(recs))
		assert.Equal(t, [][]byte{[]byte("b")}, recs[0].key)

		// THEN: ユニークインデックスからも物理削除されている (全エントリを走査)
		indexTree := btree.NewBTree(table.UniqueIndexes[0].MetaPageId)
		indexIter, err := indexTree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		var indexKeys []string
		for {
			record, ok := indexIter.Get()
			if !ok {
				break
			}
			var keyColumns [][]byte
			encode.Decode(record.KeyBytes(), &keyColumns)
			indexKeys = append(indexKeys, string(keyColumns[0]))
			_, _, err := indexIter.Next(bp)
			assert.NoError(t, err)
		}
		assert.Equal(t, []string{"Smith"}, indexKeys)
	})

	t.Run("物理削除した後に同じ PK で再挿入できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// WHEN: 物理削除してから同じ PK で再挿入
		err = table.deleteRaw(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Jane")})
		assert.NoError(t, err)

		// THEN
		recs := collectAllTablePairs(t, bp, &table)
		assert.Equal(t, 1, len(recs))
		assert.Equal(t, [][]byte{[]byte("a")}, recs[0].key)
		assert.Equal(t, [][]byte{[]byte("Jane")}, recs[0].value)
	})

	t.Run("全行を物理削除した後にテーブルが空になる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		record1 := [][]byte{[]byte("a"), []byte("John")}
		record2 := [][]byte{[]byte("b"), []byte("Alice")}
		err = table.Insert(bp, 0, lock.NewManager(5000), record1)
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), record2)
		assert.NoError(t, err)

		// WHEN
		err = table.deleteRaw(bp, 0, lock.NewManager(5000), record1)
		assert.NoError(t, err)
		err = table.deleteRaw(bp, 0, lock.NewManager(5000), record2)
		assert.NoError(t, err)

		// THEN: B+Tree を直接走査してもレコードが存在しない
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)
		_, ok := iter.Get()
		assert.False(t, ok)
	})
}

func TestUpdate(t *testing.T) {
	t.Run("プライマリキーが同じ場合、value のみが更新される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: プライマリキー "a" のレコードを更新 (キーは同じ、value のみ変更)
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane"), []byte("Doe-Updated")}
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), oldRecord, newRecord)

		// THEN
		assert.NoError(t, err)
		recs := collectAllTablePairs(t, bp, &table)
		assert.Equal(t, 2, len(recs))
		// "a" の value が更新されている
		assert.Equal(t, [][]byte{[]byte("a")}, recs[0].key)
		assert.Equal(t, [][]byte{[]byte("Jane"), []byte("Doe-Updated")}, recs[0].value)
		// "b" は変わらない
		assert.Equal(t, [][]byte{[]byte("b")}, recs[1].key)
		assert.Equal(t, [][]byte{[]byte("Alice"), []byte("Smith")}, recs[1].value)
	})

	t.Run("プライマリキーが同じ場合、B+Tree レベルでインプレース更新される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// WHEN: PK を変えずに value を更新
		oldRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane")}
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), oldRecord, newRecord)
		assert.NoError(t, err)

		// THEN: B+Tree を直接走査するとレコードは 1 件で DeleteMark=0 のまま
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		record, ok := iter.Get()
		assert.True(t, ok)
		assert.Equal(t, uint8(0), record.HeaderBytes()[0]) // active のまま

		var decodedKey [][]byte
		encode.Decode(record.KeyBytes(), &decodedKey)
		assert.Equal(t, "a", string(decodedKey[0]))

		var decodedValue [][]byte
		encode.Decode(record.NonKeyBytes(), &decodedValue)
		assert.Equal(t, "Jane", string(decodedValue[0]))

		// 2 件目は存在しない (ソフトデリートされたレコードが残っていない)
		err = iter.Advance(bp)
		assert.NoError(t, err)
		_, ok = iter.Get()
		assert.False(t, ok)
	})

	t.Run("プライマリキーが変わる場合、B+Tree レベルでソフトデリート + Insert になる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// WHEN: PK を "a" → "b" に変更
		oldRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("b"), []byte("Jane")}
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), oldRecord)
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), newRecord)
		assert.NoError(t, err)

		// THEN: B+Tree を直接走査すると 2 件存在し、"a" は DeleteMark=1、"b" は DeleteMark=0
		tree := btree.NewBTree(table.MetaPageId)
		iter, err := tree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		type entry struct {
			pk         string
			deleteMark uint8
		}
		var entries []entry
		for {
			record, ok := iter.Get()
			if !ok {
				break
			}
			var decodedKey [][]byte
			encode.Decode(record.KeyBytes(), &decodedKey)
			entries = append(entries, entry{
				pk:         string(decodedKey[0]),
				deleteMark: record.HeaderBytes()[0],
			})
			_, _, err := iter.Next(bp)
			assert.NoError(t, err)
		}

		assert.Equal(t, 2, len(entries))
		assert.Equal(t, "a", entries[0].pk)
		assert.Equal(t, uint8(1), entries[0].deleteMark) // ソフトデリート済み
		assert.Equal(t, "b", entries[1].pk)
		assert.Equal(t, uint8(0), entries[1].deleteMark) // active
	})

	t.Run("プライマリキーが変わる場合、Delete + Insert が行われる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("c"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN: プライマリキーを "a" → "b" に変更
		oldRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("b"), []byte("John-Updated")}
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), oldRecord)
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), newRecord)

		// THEN
		assert.NoError(t, err)
		recs := collectAllTablePairs(t, bp, &table)
		assert.Equal(t, 2, len(recs))
		// "a" は削除され、"b" が挿入されている
		assert.Equal(t, [][]byte{[]byte("b")}, recs[0].key)
		assert.Equal(t, [][]byte{[]byte("John-Updated")}, recs[0].value)
		assert.Equal(t, [][]byte{[]byte("c")}, recs[1].key)
		assert.Equal(t, [][]byte{[]byte("Bob")}, recs[1].value)
	})

	t.Run("ユニークインデックスのセカンダリキーが変わる場合、インデックスも更新される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", indexMetaPageId, 2, 1)

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex}, nil, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: セカンダリキー (last_name) を "Doe" → "Williams" に変更
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Williams")}
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), oldRecord, newRecord)

		// THEN: ユニークインデックスが更新されている (active なエントリのみ確認)
		assert.NoError(t, err)
		indexKeys := collectActiveUniqueIndexKeys(t, bp, table.UniqueIndexes[0])
		// "Doe" がソフトデリートされ "Williams" が追加されている
		assert.Equal(t, []string{"Smith", "Williams"}, indexKeys)
	})

	t.Run("セカンダリキーが同じでプライマリキーが変わる場合、インデックスの value が更新される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", indexMetaPageId, 2, 1)

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex}, nil, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)

		// WHEN: プライマリキーを "a" → "x" に変更、セカンダリキー "Doe" は同じ
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("x"), []byte("John"), []byte("Doe")}
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), oldRecord)
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), newRecord)

		// THEN: テーブルが更新されている
		assert.NoError(t, err)
		recs := collectAllTablePairs(t, bp, &table)
		assert.Equal(t, 1, len(recs))
		assert.Equal(t, [][]byte{[]byte("x")}, recs[0].key)

		// THEN: ユニークインデックスが更新されている
		// active なエントリのみ確認し、Key にセカンダリキーと新しい PK が含まれる
		indexTree := btree.NewBTree(table.UniqueIndexes[0].MetaPageId)
		indexIter, err := indexTree.Search(bp, btree.SearchModeStart{})
		assert.NoError(t, err)

		// active なエントリを探す
		var foundActive bool
		for {
			record, ok := indexIter.Get()
			if !ok {
				break
			}
			if record.HeaderBytes()[0] != 1 {
				var keyColumns [][]byte
				encode.Decode(record.KeyBytes(), &keyColumns)
				assert.Equal(t, "Doe", string(keyColumns[0]))
				assert.Equal(t, "x", string(keyColumns[1]))
				assert.Equal(t, 0, len(record.NonKeyBytes()))
				foundActive = true
			}
			_, _, err := indexIter.Next(bp)
			assert.NoError(t, err)
		}
		assert.True(t, foundActive, "active なインデックスエントリが存在するべき")
	})

	t.Run("プライマリキーを既存のキーに変更するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN: プライマリキーを "a" → "b" に変更 (既存のキーと衝突)
		oldRecord := [][]byte{[]byte("a"), []byte("John")}
		newRecord := [][]byte{[]byte("b"), []byte("John")}
		err = table.SoftDelete(bp, 0, lock.NewManager(5000), oldRecord)
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), newRecord)

		// THEN: SoftDelete("a") は成功するが Insert("b") が重複キーエラーで失敗する
		assert.Error(t, err)
	})

	t.Run("ユニークインデックスの更新が失敗した場合にエラーが返る", func(t *testing.T) {
		// GIVEN: セカンダリキーが重複する状況を作る
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex := NewUniqueIndex("idx_last_name", "last_name", indexMetaPageId, 2, 1)

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex}, nil, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: セカンダリキーを "Doe" → "Smith" に変更 (既存のインデックスキーと衝突)
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Smith")}
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), oldRecord, newRecord)

		// THEN: ユニークインデックスの更新でエラーが返る
		assert.Error(t, err)
	})

	t.Run("複数のユニークインデックスがある場合、すべて更新される", func(t *testing.T) {
		// GIVEN: 2 つのユニークインデックスを持つテーブル
		bp, metaPageId, _ := InitDisk(t, "users.db")

		indexMetaPageId1, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex1 := NewUniqueIndex("idx_first_name", "first_name", indexMetaPageId1, 1, 1)
		indexMetaPageId2, err := bp.AllocatePageId(metaPageId.FileId)
		assert.NoError(t, err)
		uniqueIndex2 := NewUniqueIndex("idx_last_name", "last_name", indexMetaPageId2, 2, 1)

		table := NewTable("users", metaPageId, 1, []*UniqueIndex{uniqueIndex1, uniqueIndex2}, nil, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// WHEN: 両方のセカンダリキーが変わる更新
		oldRecord := [][]byte{[]byte("a"), []byte("John"), []byte("Doe")}
		newRecord := [][]byte{[]byte("a"), []byte("Jane"), []byte("Williams")}
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), oldRecord, newRecord)
		assert.NoError(t, err)

		// THEN: idx_first_name が更新されている (active なエントリのみ)
		firstNameKeys := collectActiveUniqueIndexKeys(t, bp, table.UniqueIndexes[0])
		assert.Equal(t, []string{"Alice", "Jane"}, firstNameKeys)

		// THEN: idx_last_name が更新されている (active なエントリのみ)
		lastNameKeys := collectActiveUniqueIndexKeys(t, bp, table.UniqueIndexes[1])
		assert.Equal(t, []string{"Smith", "Williams"}, lastNameKeys)
	})

	t.Run("存在しないキーを更新するとエラーが返る", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)

		// WHEN: 存在しないキー "z" で更新
		oldRecord := [][]byte{[]byte("z"), []byte("Unknown")}
		newRecord := [][]byte{[]byte("z"), []byte("Updated")}
		err = table.UpdateInplace(bp, 0, lock.NewManager(5000), oldRecord, newRecord)

		// THEN
		assert.Error(t, err)
	})

	t.Run("対象行に排他ロックが取得される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		lockMgr := lock.NewManager(200)

		// WHEN: trx1 が UpdateInplace で排他ロックを取得
		err = table.UpdateInplace(bp, 1, lockMgr,
			[][]byte{[]byte("a"), []byte("Alice")},
			[][]byte{[]byte("a"), []byte("Updated")},
		)
		assert.NoError(t, err)

		// THEN: trx2 が同じ行に排他ロックを取得しようとするとタイムアウト
		err = table.UpdateInplace(bp, 2, lockMgr,
			[][]byte{[]byte("a"), []byte("Updated")},
			[][]byte{[]byte("a"), []byte("Updated2")},
		)
		assert.ErrorIs(t, err, lock.ErrTimeout)
	})

	t.Run("ReleaseAll 後は他のトランザクションがロックを取得できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		lockMgr := lock.NewManager(200)

		// trx1 が排他ロックを取得
		err = table.UpdateInplace(bp, 1, lockMgr,
			[][]byte{[]byte("a"), []byte("Alice")},
			[][]byte{[]byte("a"), []byte("Updated")},
		)
		assert.NoError(t, err)

		// WHEN: trx1 のロックを解放
		lockMgr.ReleaseAll(1)

		// THEN: trx2 が排他ロックを取得できる
		err = table.UpdateInplace(bp, 2, lockMgr,
			[][]byte{[]byte("a"), []byte("Updated")},
			[][]byte{[]byte("a"), []byte("Updated2")},
		)
		assert.NoError(t, err)
	})
}

func TestSearch(t *testing.T) {
	t.Run("RecordSearchModeStart で全 active レコードを取得できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN
		recs := collectAllTablePairs(t, bp, &table)

		// THEN
		assert.Equal(t, 2, len(recs))
		assert.Equal(t, [][]byte{[]byte("a")}, recs[0].key)
		assert.Equal(t, [][]byte{[]byte("b")}, recs[1].key)
	})

	t.Run("RecordSearchModeKey で指定したキーから検索できる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("c"), []byte("Bob")})
		assert.NoError(t, err)

		// WHEN: "b" から検索
		iter, err := table.Search(bp, 0, lock.NewManager(5000), RecordSearchModeKey{Key: [][]byte{[]byte("b")}})
		assert.NoError(t, err)

		columns, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "b", string(columns[0]))
		assert.Equal(t, "Alice", string(columns[1]))
	})

	t.Run("空のテーブルを検索すると結果が空になる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		// WHEN
		recs := collectAllTablePairs(t, bp, &table)

		// THEN
		assert.Equal(t, 0, len(recs))
	})

	t.Run("各行の読み取り時に共有ロックが取得される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)

		lockMgr := lock.NewManager(5000)

		// WHEN: trx1 が Search で全行を読み取る
		iter, err := table.Search(bp, 1, lockMgr, RecordSearchModeStart{})
		assert.NoError(t, err)
		for {
			_, ok, err := iter.Next()
			assert.NoError(t, err)
			if !ok {
				break
			}
		}

		// THEN: trx2 が共有ロックを取得できる (共有同士は競合しない)
		iter2, err := table.Search(bp, 2, lockMgr, RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter2.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "a", string(record[0]))
	})
}

func TestGetUniqueIndexByName(t *testing.T) {
	t.Run("インデックス名からユニークインデックスを取得できる", func(t *testing.T) {
		// GIVEN
		uniqueIndex1 := NewUniqueIndex("idx_first_name", "first_name", page.PageId{}, 1, 1)
		uniqueIndex2 := NewUniqueIndex("idx_last_name", "last_name", page.PageId{}, 2, 1)
		table := NewTable("users", page.PageId{}, 1, []*UniqueIndex{uniqueIndex1, uniqueIndex2}, nil, nil)

		// WHEN
		ui, err := table.GetUniqueIndexByName("idx_last_name")

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uniqueIndex2, ui)
	})

	t.Run("存在しないインデックス名を指定するとエラーになる", func(t *testing.T) {
		// GIVEN
		uniqueIndex := NewUniqueIndex("idx_first_name", "first_name", page.PageId{}, 1, 1)
		table := NewTable("users", page.PageId{}, 1, []*UniqueIndex{uniqueIndex}, nil, nil)

		// WHEN
		ui, err := table.GetUniqueIndexByName("idx_last_name")
		// THEN
		assert.Nil(t, ui)
		assert.Error(t, err)
	})
}

func InitDisk(t *testing.T, pathname string) (bufferPool *buffer.BufferPool, metaPageId page.PageId, tmpdir string) {
	tmpdir = t.TempDir()
	filePath := filepath.Join(tmpdir, pathname)

	bp := buffer.NewBufferPool(10, nil)
	fileId := page.FileId(1)
	dm, err := file.NewDisk(fileId, filePath)
	assert.NoError(t, err)
	bp.RegisterDisk(fileId, dm)

	// metaPageId を割り当て
	metaPageId, err = bp.AllocatePageId(fileId)
	assert.NoError(t, err)

	return bp, metaPageId, tmpdir
}

func TestLeafPageCount(t *testing.T) {
	t.Run("作成直後のテーブルのリーフページ数は1", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		// WHEN
		count, err := table.LeafPageCount(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})

	t.Run("データ挿入によりリーフページが分割されるとリーフページ数が増加する", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		// WHEN: 十分な量のデータを挿入してリーフページ分割を発生させる
		for i := range 200 {
			key := fmt.Sprintf("key_%04d", i)
			value := fmt.Sprintf("value_%04d", i)
			err := table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte(key), []byte(value)})
			assert.NoError(t, err)
		}

		// THEN
		count, err := table.LeafPageCount(bp)
		assert.NoError(t, err)
		assert.Greater(t, count, uint64(1))
	})
}

func TestHeight(t *testing.T) {
	t.Run("作成直後のテーブルの高さは1", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		// WHEN
		height, err := table.Height(bp)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})

	t.Run("データ挿入によりリーフページが分割されるとテーブルの高さが増加する", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		// WHEN: 十分な量のデータを挿入してリーフページ分割を発生させる
		for i := range 200 {
			key := fmt.Sprintf("key_%04d", i)
			value := fmt.Sprintf("value_%04d", i)
			err := table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte(key), []byte(value)})
			assert.NoError(t, err)
		}

		// THEN
		height, err := table.Height(bp)
		assert.NoError(t, err)
		assert.Greater(t, height, uint64(1))
	})
}

// テーブルの全 active レコードをデコードして収集するヘルパー
//
// ClusteredIndexIterator を使用するため、ソフトデリート済みレコードはスキップされる
type decodedRecord struct {
	key   [][]byte
	value [][]byte
}

func collectAllTablePairs(t *testing.T, bp *buffer.BufferPool, table *Table) []decodedRecord {
	t.Helper()
	iter, err := table.Search(bp, 0, lock.NewManager(5000), RecordSearchModeStart{})
	assert.NoError(t, err)

	var records []decodedRecord
	for {
		columns, ok, err := iter.Next()
		assert.NoError(t, err)
		if !ok {
			break
		}
		// columns は [PK..., NonKey...] のフラット配列
		key := columns[:table.PrimaryKeyCount]
		value := columns[table.PrimaryKeyCount:]
		records = append(records, decodedRecord{key: key, value: value})
	}
	return records
}

func TestUndoLogRecording(t *testing.T) {
	t.Run("Insert 時に UndoInsertRecord が記録される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, tmpdir := InitDisk(t, "users.db")
		undoDm, err := file.NewDisk(undoTestFileId, filepath.Join(tmpdir, "undo.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(undoTestFileId, undoDm)
		undoLog, err := NewUndoLog(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := NewTable("users", metaPageId, 1, nil, undoLog, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		var trxId lock.TrxId = 1

		// WHEN
		err = table.Insert(bp, trxId, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// THEN
		records := undoLog.GetRecords(trxId)
		assert.Equal(t, 1, len(records))
		insertRecord, ok := records[0].(UndoInsertRecord)
		assert.True(t, ok)
		assert.Equal(t, []byte("a"), insertRecord.Record[0])
		assert.Equal(t, []byte("Alice"), insertRecord.Record[1])
	})

	t.Run("SoftDelete 時に UndoDeleteRecord が記録される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, tmpdir := InitDisk(t, "users.db")
		undoDm, err := file.NewDisk(undoTestFileId, filepath.Join(tmpdir, "undo.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(undoTestFileId, undoDm)
		undoLog, err := NewUndoLog(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := NewTable("users", metaPageId, 1, nil, undoLog, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		var trxId lock.TrxId = 1
		lockMgr := lock.NewManager(5000)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN
		err = table.SoftDelete(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// THEN
		records := undoLog.GetRecords(trxId)
		assert.Equal(t, 2, len(records)) // Insert + SoftDelete
		deleteRecord, ok := records[1].(UndoDeleteRecord)
		assert.True(t, ok)
		assert.Equal(t, []byte("a"), deleteRecord.Record[0])
		assert.Equal(t, []byte("Alice"), deleteRecord.Record[1])
	})

	t.Run("UpdateInplace 時に UndoUpdateInplaceRecord が記録される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, tmpdir := InitDisk(t, "users.db")
		undoDm, err := file.NewDisk(undoTestFileId, filepath.Join(tmpdir, "undo.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(undoTestFileId, undoDm)
		undoLog, err := NewUndoLog(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := NewTable("users", metaPageId, 1, nil, undoLog, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		var trxId lock.TrxId = 1
		lockMgr := lock.NewManager(5000)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN
		err = table.UpdateInplace(bp, trxId, lockMgr,
			[][]byte{[]byte("a"), []byte("Alice")},
			[][]byte{[]byte("a"), []byte("Bob")},
		)
		assert.NoError(t, err)

		// THEN
		records := undoLog.GetRecords(trxId)
		assert.Equal(t, 2, len(records)) // Insert + UpdateInplace
		updateRecord, ok := records[1].(UndoUpdateInplaceRecord)
		assert.True(t, ok)
		assert.Equal(t, []byte("Alice"), updateRecord.PrevRecord[1])
		assert.Equal(t, []byte("Bob"), updateRecord.NewRecord[1])
	})

	t.Run("undoLog が nil の場合は Undo 記録がスキップされる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})

		// THEN: undoLog が nil でもパニックしない
		assert.NoError(t, err)
	})

	t.Run("複数操作で Undo ログが操作順に記録される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, tmpdir := InitDisk(t, "users.db")
		undoDm, err := file.NewDisk(undoTestFileId, filepath.Join(tmpdir, "undo.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(undoTestFileId, undoDm)
		undoLog, err := NewUndoLog(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := NewTable("users", metaPageId, 1, nil, undoLog, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		var trxId lock.TrxId = 1
		lockMgr := lock.NewManager(5000)

		// WHEN: Insert → UpdateInplace → Insert → SoftDelete
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		err = table.UpdateInplace(bp, trxId, lockMgr,
			[][]byte{[]byte("a"), []byte("Alice")},
			[][]byte{[]byte("a"), []byte("Carol")},
		)
		assert.NoError(t, err)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)
		err = table.SoftDelete(bp, trxId, lockMgr, [][]byte{[]byte("b"), []byte("Bob")})
		assert.NoError(t, err)

		// THEN
		records := undoLog.GetRecords(trxId)
		assert.Equal(t, 4, len(records))
		_, ok := records[0].(UndoInsertRecord)
		assert.True(t, ok)
		_, ok = records[1].(UndoUpdateInplaceRecord)
		assert.True(t, ok)
		_, ok = records[2].(UndoInsertRecord)
		assert.True(t, ok)
		_, ok = records[3].(UndoDeleteRecord)
		assert.True(t, ok)
	})

	t.Run("Insert が重複キーで失敗した場合、Undo ログが PopLast される", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, tmpdir := InitDisk(t, "users.db")
		undoDm, err := file.NewDisk(undoTestFileId, filepath.Join(tmpdir, "undo.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(undoTestFileId, undoDm)
		undoLog, err := NewUndoLog(bp, nil, undoTestFileId)
		assert.NoError(t, err)
		table := NewTable("users", metaPageId, 1, nil, undoLog, nil)
		err = table.Create(bp)
		assert.NoError(t, err)

		var trxId lock.TrxId = 1
		lockMgr := lock.NewManager(5000)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN: 同じ PK で Insert (重複キーエラー)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Bob")})

		// THEN: エラーが返り、Undo ログには最初の Insert のみが残る
		assert.Error(t, err)
		records := undoLog.GetRecords(trxId)
		assert.Equal(t, 1, len(records))
	})
}

func TestInsertRedoLog(t *testing.T) {
	t.Run("Insert 後にデータページの REDO ログが記録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		redoLog, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(10, redoLog)
		fileId := page.FileId(1)
		dm, err := file.NewDisk(fileId, filepath.Join(tmpdir, "users.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(fileId, dm)
		metaPageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		table := NewTable("users", metaPageId, 1, nil, nil, redoLog)
		err = table.Create(bp)
		assert.NoError(t, err)

		// Create で作られたダーティーページをフラッシュしてクリアする
		err = bp.FlushPage()
		assert.NoError(t, err)
		err = redoLog.Reset()
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(bp, 1, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// THEN
		err = redoLog.Flush()
		assert.NoError(t, err)
		records, err := redoLog.ReadAll()
		assert.NoError(t, err)
		assert.Greater(t, len(records), 0)

		// 全レコードがページ変更レコードであること
		for _, rec := range records {
			assert.Equal(t, log.RedoPageWrite, rec.Type)
			assert.Equal(t, uint64(1), rec.TrxId)
			assert.Equal(t, page.PAGE_SIZE, len(rec.Data))
		}
	})

	t.Run("redoLog が nil の場合は REDO 記録がスキップされる", func(t *testing.T) {
		// GIVEN
		bp, metaPageId, _ := InitDisk(t, "users.db")
		table := NewTable("users", metaPageId, 1, nil, nil, nil)
		err := table.Create(bp)
		assert.NoError(t, err)

		// WHEN / THEN: redoLog が nil でもパニックしない
		err = table.Insert(bp, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
	})
}

func TestSoftDeleteRedoLog(t *testing.T) {
	t.Run("SoftDelete 後にデータページの REDO ログが記録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		redoLog, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(10, redoLog)
		fileId := page.FileId(1)
		dm, err := file.NewDisk(fileId, filepath.Join(tmpdir, "users.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(fileId, dm)
		metaPageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		table := NewTable("users", metaPageId, 1, nil, nil, redoLog)
		err = table.Create(bp)
		assert.NoError(t, err)

		lockMgr := lock.NewManager(5000)
		err = table.Insert(bp, 1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// ダーティーページをフラッシュしてクリアする
		err = bp.FlushPage()
		assert.NoError(t, err)
		err = redoLog.Reset()
		assert.NoError(t, err)

		// WHEN
		err = table.SoftDelete(bp, 1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// THEN
		err = redoLog.Flush()
		assert.NoError(t, err)
		records, err := redoLog.ReadAll()
		assert.NoError(t, err)
		assert.Greater(t, len(records), 0)

		lastRecord := records[len(records)-1]
		assert.Equal(t, log.RedoPageWrite, lastRecord.Type)
		assert.Equal(t, uint64(1), lastRecord.TrxId)
	})
}

func TestUpdateInplaceRedoLog(t *testing.T) {
	t.Run("UpdateInplace 後にデータページの REDO ログが記録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		redoLog, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(10, redoLog)
		fileId := page.FileId(1)
		dm, err := file.NewDisk(fileId, filepath.Join(tmpdir, "users.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(fileId, dm)
		metaPageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		table := NewTable("users", metaPageId, 1, nil, nil, redoLog)
		err = table.Create(bp)
		assert.NoError(t, err)

		lockMgr := lock.NewManager(5000)
		err = table.Insert(bp, 1, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// ダーティーページをフラッシュしてクリアする
		err = bp.FlushPage()
		assert.NoError(t, err)
		err = redoLog.Reset()
		assert.NoError(t, err)

		// WHEN
		err = table.UpdateInplace(bp, 1, lockMgr,
			[][]byte{[]byte("a"), []byte("Alice")},
			[][]byte{[]byte("a"), []byte("Bob")})
		assert.NoError(t, err)

		// THEN
		err = redoLog.Flush()
		assert.NoError(t, err)
		records, err := redoLog.ReadAll()
		assert.NoError(t, err)
		assert.Greater(t, len(records), 0)
	})
}

// ユニークインデックスの active なエントリのセカンダリキーを収集するヘルパー
//
// ソフトデリート済み (DeleteMark=1) のエントリはスキップする
func collectActiveUniqueIndexKeys(t *testing.T, bp *buffer.BufferPool, ui *UniqueIndex) []string {
	t.Helper()
	indexTree := btree.NewBTree(ui.MetaPageId)
	indexIter, err := indexTree.Search(bp, btree.SearchModeStart{})
	assert.NoError(t, err)

	var keys []string
	for {
		record, ok := indexIter.Get()
		if !ok {
			break
		}
		if record.HeaderBytes()[0] != 1 {
			var keyColumns [][]byte
			encode.Decode(record.KeyBytes(), &keyColumns)
			keys = append(keys, string(keyColumns[0]))
		}
		_, _, err := indexIter.Next(bp)
		assert.NoError(t, err)
	}
	return keys
}

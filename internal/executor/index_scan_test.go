package executor

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIndexScan(t *testing.T) {
	t.Run("正常に IndexScan を作成できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		tbl := InitTableForIndexScan(t, bpm)
		uniqueIndexTree := btree.NewBTree(tbl.UniqueIndexes[0].MetaPageId)

		// GIVEN
		indexIterator, err := uniqueIndexTree.Search(bpm, btree.SearchModeStart{})
		assert.NoError(t, err)
		whileCondition := func(record Record) bool {
			return true
		}

		// WHEN
		indexScan := NewIndexScan(
			tbl.MetaPageId,
			indexIterator,
			whileCondition,
		)

		// THEN
		assert.NotNil(t, indexScan)
	})
}

func TestIndexScan(t *testing.T) {
	t.Run("インデックスでスキャンできる (SearchModeStart を使用)", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		tbl := InitTableForIndexScan(t, bpm)
		uniqueIndexTree := btree.NewBTree(tbl.UniqueIndexes[0].MetaPageId)
		indexIterator, _ := uniqueIndexTree.Search(bpm, btree.SearchModeStart{})

		// GIVEN
		indexScan := NewIndexScan(
			tbl.MetaPageId,
			indexIterator,
			func(record Record) bool {
				return string(record[0]) < "J" // セカンダリキー (姓) が "J" 未満の間、継続
			},
		)

		// WHEN
		var results []Record
		for {
			record, err := indexScan.Next(bpm)
			assert.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		expected := []Record{
			{[]byte("e"), []byte("Charlie"), []byte("Brown")},
			{[]byte("d"), []byte("Eve"), []byte("Davis")},
			{[]byte("a"), []byte("John"), []byte("Doe")},
		}
		assert.Equal(t, expected, results)
	})

	t.Run("インデックスでスキャンできる (SearchModeKey を使用)", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		tbl := InitTableForIndexScan(t, bpm)
		uniqueIndexTree := btree.NewBTree(tbl.UniqueIndexes[0].MetaPageId)
		indexIterator, _ := uniqueIndexTree.Search(bpm, btree.SearchModeKey{Key: []byte("Doe")})

		// GIVEN
		indexScan := NewIndexScan(
			tbl.MetaPageId,
			indexIterator,
			func(record Record) bool {
				return string(record[0]) <= "Smith" // セカンダリキー (姓) が "Smith" 以下の間、継続
			},
		)

		// WHEN
		var results []Record
		for {
			record, err := indexScan.Next(bpm)
			assert.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		expected := []Record{
			{[]byte("a"), []byte("John"), []byte("Doe")},
			{[]byte("c"), []byte("Bob"), []byte("Johnson")},
			{[]byte("b"), []byte("Alice"), []byte("Smith")},
		}
		assert.Equal(t, expected, results)
	})
}

func InitTableForIndexScan(t *testing.T, bpm *bufferpool.BufferPoolManager) table.Table {
	uniqueIndexes := table.NewUniqueIndex(disk.OLD_INVALID_PAGE_ID, 2)
	tbl := table.NewTable(disk.OldPageId(0), 1, []*table.UniqueIndex{uniqueIndexes})

	// テーブルを作成
	err := tbl.Create(bpm)
	assert.NoError(t, err)

	// 行を挿入
	err = tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	err = tbl.Insert(bpm, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	err = tbl.Insert(bpm, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	err = tbl.Insert(bpm, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	err = tbl.Insert(bpm, [][]byte{[]byte("e"), []byte("Charlie"), []byte("Brown")})
	assert.NoError(t, err)

	return tbl
}

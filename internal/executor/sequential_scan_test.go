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

func TestNewSequentialScan(t *testing.T) {
	t.Run("正常に SequentialScan を作成できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		table := InitTable(t, bpm)
		btr := btree.NewBTree(table.MetaPageId)

		// GIVEN
		tableIterator, err := btr.Search(bpm, btree.SearchModeStart{})
		assert.NoError(t, err)
		whileCondition := func(record Record) bool {
			return true
		}

		// WHEN
		seqScan := NewSequentialScan(
			tableIterator,
			whileCondition,
		)

		// THEN
		assert.NotNil(t, seqScan)
	})
}

func TestSequentialScan(t *testing.T) {
	t.Run("テーブルをシーケンシャルスキャンできる (SearchModeStart を使用)", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		table := InitTable(t, bpm)
		btr := btree.NewBTree(table.MetaPageId)
		tableIterator, _ := btr.Search(bpm, btree.SearchModeStart{})

		// GIVEN
		seqScan := NewSequentialScan(
			tableIterator,
			func(record Record) bool {
				return string(record[0]) < "c" // プライマリキーが "c" 未満の間、継続
			},
		)

		// WHEN
		var results []Record
		for {
			record, err := seqScan.Next(bpm)
			assert.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		expected := []Record{
			{[]byte("a"), []byte("John"), []byte("Doe")},
			{[]byte("b"), []byte("Alice"), []byte("Smith")},
		}
		assert.Equal(t, expected, results)
	})

	t.Run("テーブルをシーケンシャルスキャンできる (SearchModeKey を使用)", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		table := InitTable(t, bpm)
		btr := btree.NewBTree(table.MetaPageId)
		tableIterator, _ := btr.Search(bpm, btree.SearchModeKey{Key: []byte("b")})

		// GIVEN
		seqScan := NewSequentialScan(
			tableIterator,
			func(record Record) bool {
				return string(record[0]) <= "d" // プライマリキーが d" 以下の間、継続
			},
		)

		// WHEN
		var results []Record
		for {
			record, err := seqScan.Next(bpm)
			assert.NoError(t, err)
			if record == nil {
				break
			}
			results = append(results, record)
		}

		// THEN
		expected := []Record{
			{[]byte("b"), []byte("Alice"), []byte("Smith")},
			{[]byte("c"), []byte("Bob"), []byte("Johnson")},
			{[]byte("d"), []byte("Eve"), []byte("Davis")},
		}
		assert.Equal(t, expected, results)
	})
}

func InitTable(t *testing.T, bpm *bufferpool.BufferPoolManager) table.Table {
	uniqueIndexes := table.NewUniqueIndex(disk.INVALID_PAGE_ID, 2)
	table := table.NewTable(disk.PageId(0), 1, []*table.UniqueIndex{uniqueIndexes})

	// テーブルを作成
	err := table.Create(bpm)
	assert.NoError(t, err)

	// 行を挿入
	err = table.Insert(bpm, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	err = table.Insert(bpm, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	err = table.Insert(bpm, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	err = table.Insert(bpm, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	err = table.Insert(bpm, [][]byte{[]byte("e"), []byte("Charlie"), []byte("Brown")})
	assert.NoError(t, err)

	return table
}

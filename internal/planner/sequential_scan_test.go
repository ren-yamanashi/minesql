package planner

import (
	"minesql/internal/executor"
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"minesql/internal/storage/bufferpool"
	"minesql/internal/storage/disk"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSequentialScan(t *testing.T) {
	t.Run("正常に SequentialScan が生成される", func(t *testing.T) {
		// GIVEN
		tableMetaPageId := disk.PageId(42)
		searchMode := btree.SearchModeStart{}
		whileCondition := func(record executor.Record) bool {
			return true
		}

		// WHEN
		ss := NewSequentialScan(tableMetaPageId, searchMode, whileCondition)

		// THEN
		assert.Equal(t, tableMetaPageId, ss.TableMetaPageId)
		assert.Equal(t, searchMode, ss.SearchMode)
	})

	t.Run("Start で Executor が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		table := InitTable(t, bpm)

		// GIVEN
		searchMode := btree.SearchModeStart{}
		whileCondition := func(record executor.Record) bool {
			return true
		}
		ss := NewSequentialScan(table.MetaPageId, searchMode, whileCondition)

		// WHEN
		exec, err := ss.Start(bpm)

		// THEN
		assert.NoError(t, err)
		assert.IsType(t, &executor.SequentialScan{}, exec)
	})
}

func InitTable(t *testing.T, bpm *bufferpool.BufferPoolManager) table.Table {
	table := table.Table{
		MetaPageId:      disk.PageId(0),
		PrimaryKeyCount: 1,
	}

	// テーブルを作成
	err := table.Create(bpm)
	assert.NoError(t, err)

	// 行を挿入
	err = table.Insert(bpm, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	err = table.Insert(bpm, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	err = table.Insert(bpm, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	err = table.Insert(bpm, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	assert.NoError(t, err)

	return table
}

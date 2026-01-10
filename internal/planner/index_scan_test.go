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

func TestNewIndexScan(t *testing.T) {
	t.Run("正常に IndexScan が生成される", func(t *testing.T) {
		// GIVEN
		tableMetaPageId := disk.PageId(42)
		indexMetaPageId := disk.PageId(43)
		searchMode := btree.SearchModeStart{}
		whileCondition := func(record executor.Record) bool {
			return true
		}

		// WHEN
		is := NewIndexScan(tableMetaPageId, indexMetaPageId, searchMode, whileCondition)

		// THEN
		assert.Equal(t, tableMetaPageId, is.TableMetaPageId)
		assert.Equal(t, indexMetaPageId, is.IndexMetaPageId)
		assert.Equal(t, searchMode, is.SearchMode)
	})

	t.Run("Start で Executor が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		tbl := InitTableForIndexScanPlanner(t, bpm)

		// GIVEN
		searchMode := btree.SearchModeStart{}
		whileCondition := func(record executor.Record) bool {
			return true
		}
		is := NewIndexScan(tbl.MetaPageId, tbl.UniqueIndexes[0].MetaPageId, searchMode, whileCondition)

		// WHEN
		exec, err := is.Start(bpm)

		// THEN
		assert.NoError(t, err)
		assert.IsType(t, &executor.IndexScan{}, exec)
	})
}

func InitTableForIndexScanPlanner(t *testing.T, bpm *bufferpool.BufferPoolManager) table.Table {
	uniqueIndexes := table.NewUniqueIndex(disk.INVALID_PAGE_ID, 2)
	tbl := table.NewTable(disk.PageId(0), 1, []*table.UniqueIndex{uniqueIndexes})

	// テーブルを作成
	err := tbl.Create(bpm)
	assert.NoError(t, err)

	// 行を挿入
	err = tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	err = tbl.Insert(bpm, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	err = tbl.Insert(bpm, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	err = tbl.Insert(bpm, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	assert.NoError(t, err)

	return tbl
}

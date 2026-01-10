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

func TestNewFilter(t *testing.T) {
	t.Run("正常に Filter が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		table := InitTable(t, bpm)

		// GIVEN
		innerPlan := InitSequentialScanPlan(t, table)
		condition := func(record executor.Record) bool {
			return string(record[0]) == "a"
		}

		// WHEN
		filter := NewFilter(&innerPlan, condition)

		// THEN
		assert.NotNil(t, filter)
	})

	t.Run("Start で Executor が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		path := filepath.Join(tmpdir, "test.db")
		dm, _ := disk.NewDiskManager(path)
		bpm := bufferpool.NewBufferPoolManager(dm, 10)
		table := InitTable(t, bpm)

		// GIVEN
		innerPlan := InitSequentialScanPlan(t, table)
		condition := func(record executor.Record) bool {
			return string(record[0]) == "a"
		}
		filter := NewFilter(&innerPlan, condition)

		// WHEN
		exec, err := filter.Start(bpm)

		// THEN
		assert.NoError(t, err)
		assert.IsType(t, &executor.Filter{}, exec)
	})
}

// すべてのレコードをスキャンする SequentialScan Plan を初期化
func InitSequentialScanPlan(t *testing.T, table table.Table) SequentialScan {
	whileCondition := func(record executor.Record) bool {
		return true
	}
	seqScan := NewSequentialScan(
		table.MetaPageId,
		btree.SearchModeStart{},
		whileCondition,
	)

	return seqScan
}

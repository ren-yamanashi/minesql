package planner

import (
	"minesql/internal/executor"
	"minesql/internal/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFilter(t *testing.T) {
	t.Run("正常に Filter が生成される", func(t *testing.T) {
		// GIVEN
		innerPlan := NewSequentialScan(
			"users",
			executor.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
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
		InitStorageEngineForPlannerTest(t, tmpdir)
		defer storage.ResetStorageEngine()

		// GIVEN
		innerPlan := NewSequentialScan(
			"users",
			executor.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		condition := func(record executor.Record) bool {
			return string(record[0]) == "a"
		}
		filter := NewFilter(&innerPlan, condition)

		// WHEN
		exec := filter.Start()

		// THEN
		assert.IsType(t, &executor.Filter{}, exec)
	})
}

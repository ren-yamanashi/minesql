package planner

import (
	"minesql/internal/executor"
	"minesql/internal/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIndexScan(t *testing.T) {
	t.Run("正常に IndexScan が生成される", func(t *testing.T) {
		// GIVEN
		tableName := "users"
		indexName := "last_name"
		searchMode := executor.RecordSearchModeStart{}
		whileCondition := func(record executor.Record) bool {
			return true
		}

		// WHEN
		is := NewIndexScan(tableName, indexName, searchMode, whileCondition)

		// THEN
		assert.Equal(t, tableName, is.TableName)
		assert.Equal(t, indexName, is.IndexName)
		assert.Equal(t, searchMode, is.SearchMode)
	})

	t.Run("Start で Executor が生成される", func(t *testing.T) {
		tmpdir := t.TempDir()
		InitStorageEngineForPlannerTest(t, tmpdir)
		defer storage.ResetStorageEngine()

		// GIVEN
		searchMode := executor.RecordSearchModeStart{}
		whileCondition := func(record executor.Record) bool {
			return true
		}
		is := NewIndexScan("users", "last_name", searchMode, whileCondition)

		// WHEN
		exec := is.Start()

		// THEN
		assert.IsType(t, &executor.IndexScan{}, exec)
	})
}

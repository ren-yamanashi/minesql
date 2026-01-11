package planner

import (
	"minesql/internal/executor"
	"minesql/internal/storage"
	"minesql/internal/storage/access/table"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSequentialScan(t *testing.T) {
	t.Run("正常に SequentialScan が生成される", func(t *testing.T) {
		// GIVEN
		tableName := "users"
		searchMode := executor.RecordSearchModeStart{}
		whileCondition := func(record executor.Record) bool {
			return true
		}

		// WHEN
		ss := NewSequentialScan(tableName, searchMode, whileCondition)

		// THEN
		assert.Equal(t, tableName, ss.TableName)
		assert.Equal(t, searchMode, ss.SearchMode)
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
		ss := NewSequentialScan("users", searchMode, whileCondition)

		// WHEN
		exec := ss.Start()

		// THEN
		assert.IsType(t, &executor.SequentialScan{}, exec)
	})
}

func InitStorageEngineForPlannerTest(t *testing.T, dataDir string) *storage.StorageEngine {
	t.Setenv("MINESQL_DATA_DIR", dataDir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")

	storage.InitStorageEngine()
	engine := storage.GetStorageEngine()

	// テーブルを作成
	uniqueIndexes := table.NewUniqueIndex("last_name", 2)
	createTable := executor.NewCreateTable()
	err := createTable.Execute("users", 1, []*table.UniqueIndex{uniqueIndexes})
	assert.NoError(t, err)

	tbl, err := engine.GetTable("users")
	assert.NoError(t, err)

	bpm := engine.GetBufferPoolManager()

	// 行を挿入
	err = tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
	assert.NoError(t, err)
	err = tbl.Insert(bpm, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
	assert.NoError(t, err)
	err = tbl.Insert(bpm, [][]byte{[]byte("c"), []byte("Bob"), []byte("Johnson")})
	assert.NoError(t, err)
	err = tbl.Insert(bpm, [][]byte{[]byte("d"), []byte("Eve"), []byte("Davis")})
	assert.NoError(t, err)

	return engine
}

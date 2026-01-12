package executor

import (
	"minesql/internal/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewInsert(t *testing.T) {
	t.Run("正常に Insert Executor を生成できる", func(t *testing.T) {
		// WHEN
		insert := NewInsert("users")

		// THEN
		assert.NotNil(t, insert)
		assert.Equal(t, "users", insert.tableName)
	})
}

func TestExecute(t *testing.T) {
	t.Run("存在しないテーブルに対して挿入しようとするとエラーになる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		defer storage.ResetStorageManager()

		insert := NewInsert("non_existent_table")

		// WHEN
		err := insert.Execute([][]byte{[]byte("1"), []byte("Alice")})

		// THEN
		assert.Error(t, err)
	})

	t.Run("正常にレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		defer storage.ResetStorageManager()

		createTable := NewCreateTable()
		err := createTable.Execute("users", 1, []*IndexParam{})
		assert.NoError(t, err)
		insert := NewInsert("users")

		// WHEN
		err = insert.Execute([][]byte{[]byte("1"), []byte("Alice")})

		// THEN
		assert.NoError(t, err)
		whileCondition := func(record Record) bool {
			return true
		}
		seqScan := NewSequentialScan(
			"users",
			RecordSearchModeStart{},
			whileCondition,
		)
		record, err := seqScan.Next()
		assert.NoError(t, err)
		assert.NotNil(t, record)
		assert.Equal(t, []byte("1"), record[0])
		assert.Equal(t, []byte("Alice"), record[1])
	})
}

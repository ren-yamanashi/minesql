package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/catalog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewInsert(t *testing.T) {
	t.Run("正常に Insert Executor を生成できる", func(t *testing.T) {
		// GIVEN
		tableName := "users"
		cols := []string{"id", "name"}
		records := [][][]byte{
			{[]byte("1"), []byte("Alice")},
			{[]byte("2"), []byte("Bob")},
		}

		// WHEN
		insert := NewInsert(tableName, cols, records)

		// THEN
		assert.NotNil(t, insert)
		assert.Equal(t, tableName, insert.tableName)
		assert.Equal(t, cols, insert.colNames)
		assert.Equal(t, records, insert.records)
	})
}

func TestExecute(t *testing.T) {
	t.Run("存在しないテーブルに対して挿入しようとするとエラーになる", func(t *testing.T) {
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		defer storage.ResetStorageManager()

		// GIVEN
		tableName := "non_existent_table"
		cols := []string{"id", "name"}
		records := [][][]byte{
			{[]byte("1"), []byte("Alice")},
		}

		// WHEN
		insert := NewInsert(tableName, cols, records)
		_, err := insert.Next()

		// THEN
		assert.Error(t, err)
	})

	t.Run("カラム名がテーブルのカラムと一致しない場合、エラーになる", func(t *testing.T) {
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		defer storage.ResetStorageManager()

		tableName := "users"
		createTable := NewCreateTable(tableName, 1, nil, []*ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		})
		_, err := createTable.Next()
		assert.NoError(t, err)

		// GIVEN
		cols := []string{"id", "email"} // "email" は存在しないカラム
		records := [][][]byte{
			{[]byte("1"), []byte("alice@example.com")},
		}

		// WHEN
		insert := NewInsert(tableName, cols, records)
		_, err = insert.Next()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column name does not match")
	})

	t.Run("正常にレコードを挿入できる", func(t *testing.T) {
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		defer storage.ResetStorageManager()

		tableName := "users"
		createTable := NewCreateTable(tableName, 1, []*IndexParam{
			{Name: "name", SecondaryKey: 1},
		}, []*ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		})
		_, err := createTable.Next()
		assert.NoError(t, err)

		// GIVEN
		cols := []string{"id", "name"}
		records := [][][]byte{
			{[]byte("1"), []byte("Alice")},
			{[]byte("2"), []byte("Bob")},
		}

		// WHEN
		insert := NewInsert(tableName, cols, records)
		_, err = insert.Next()

		// THEN
		assert.NoError(t, err)
		whileCondition := func(record Record) bool {
			return true
		}
		seqScan := NewSequentialScan(
			tableName,
			RecordSearchModeStart{},
			whileCondition,
		)
		res, err := ExecutePlan(seqScan)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(res))
		for i, record := range res {
			assert.Equal(t, records[i][0], record[0])
			assert.Equal(t, records[i][1], record[1])
		}
	})
}

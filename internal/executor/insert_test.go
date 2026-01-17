package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/catalog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewInsert(t *testing.T) {
	t.Run("正常に Insert Executor を生成できる", func(t *testing.T) {
		// WHEN
		records := [][][]byte{
			{[]byte("1"), []byte("Alice")},
			{[]byte("2"), []byte("Bob")},
		}
		insert := NewInsert("users", records)

		// THEN
		assert.NotNil(t, insert)
		assert.Equal(t, "users", insert.tableName)
		assert.Equal(t, records, insert.records)
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

		records := [][][]byte{
			{[]byte("1"), []byte("Alice")},
		}
		insert := NewInsert("non_existent_table", records)

		// WHEN
		_, err := insert.Next()

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

		createTable := NewCreateTable("users", 1, []*IndexParam{
			{Name: "name", SecondaryKey: 1},
		}, []*ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		})
		_, err := createTable.Next()
		assert.NoError(t, err)
		records := [][][]byte{
			{[]byte("1"), []byte("Alice")},
			{[]byte("2"), []byte("Bob")},
		}
		insert := NewInsert("users", records)

		// WHEN
		_, err = insert.Next()

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
		res, err := ExecutePlan(seqScan)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(res))
		for i, record := range res {
			assert.Equal(t, records[i][0], record[0])
			assert.Equal(t, records[i][1], record[1])
		}
	})
}

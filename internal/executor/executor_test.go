package executor

import (
	"minesql/internal/storage"
	"minesql/internal/storage/access/catalog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecutePlan(t *testing.T) {
	t.Run("複数のレコードを返す executor の場合、全てのレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer storage.ResetStorageManager()

		seqScan := NewSequentialScan(
			"users",
			RecordSearchModeStart{},
			func(record Record) bool {
				return true
			},
		)

		// WHEN
		results, err := ExecutePlan(seqScan)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 5, len(results))
		assert.Equal(t, []byte("a"), results[0][0])
		assert.Equal(t, []byte("e"), results[4][0])
	})

	t.Run("条件によってフィルタされたレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer storage.ResetStorageManager()

		seqScan := NewSequentialScan(
			"users",
			RecordSearchModeStart{},
			func(record Record) bool {
				return string(record[0]) < "c"
			},
		)

		// WHEN
		results, err := ExecutePlan(seqScan)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(results))
		assert.Equal(t, []byte("a"), results[0][0])
		assert.Equal(t, []byte("b"), results[1][0])
	})

	t.Run("レコードを返さない executor (Insert) の場合", func(t *testing.T) {
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		storage.ResetStorageManager()
		storage.InitStorageManager()
		defer storage.ResetStorageManager()

		tableName := "users"
		createTable := NewCreateTable(
			tableName,
			1,
			[]*IndexParam{
				{Name: "name", SecondaryKey: 1},
			},
			[]*ColumnParam{
				{Name: "id", Type: catalog.ColumnTypeString},
				{Name: "name", Type: catalog.ColumnTypeString},
			})
		_, err := createTable.Next()
		assert.NoError(t, err)

		// GIVEN
		cols := []string{"id", "name"}
		records := [][][]byte{
			{[]byte("1"), []byte("Alice")},
		}

		// WHEN
		insert := NewInsert(tableName, cols, records)
		results, err := ExecutePlan(insert)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, results)
	})
}

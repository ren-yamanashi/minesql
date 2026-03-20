package executor

import (
	"minesql/internal/access"
	"minesql/internal/catalog"
	"minesql/internal/engine"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFetchAll(t *testing.T) {
	t.Run("複数のレコードを返す RecordIterator の場合、全てのレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer engine.Reset()

		seqScan := NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool {
				return true
			},
		)

		// WHEN
		results, err := FetchAll(seqScan)

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
		defer engine.Reset()

		seqScan := NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record Record) bool {
				return string(record[0]) < "c"
			},
		)

		// WHEN
		results, err := FetchAll(seqScan)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(results))
		assert.Equal(t, []byte("a"), results[0][0])
		assert.Equal(t, []byte("b"), results[1][0])
	})

	t.Run("Mutator (Insert) は FetchAll ではなく Execute で実行する", func(t *testing.T) {
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		engine.Reset()
		engine.Init()
		defer engine.Reset()

		tableName := "users"
		createTable := NewCreateTable(
			tableName,
			1,
			[]*IndexParam{
				{Name: "name", ColName: "name", SecondaryKey: 1},
			},
			[]*ColumnParam{
				{Name: "id", Type: catalog.ColumnTypeString},
				{Name: "name", Type: catalog.ColumnTypeString},
			})
		err := createTable.Execute()
		assert.NoError(t, err)

		// GIVEN
		cols := []string{"id", "name"}
		records := []Record{
			{[]byte("1"), []byte("Alice")},
		}

		// WHEN
		insert := NewInsert(tableName, cols, records)
		err = insert.Execute()

		// THEN
		assert.NoError(t, err)

		// 挿入されたレコードを確認
		scan := NewSearchTable(tableName, access.RecordSearchModeStart{}, func(record Record) bool { return true })
		results, err := FetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
	})
}

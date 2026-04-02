package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewInsert(t *testing.T) {
	t.Run("正常に Insert Executor を生成できる", func(t *testing.T) {
		// GIVEN
		var trxId handler.TrxId = 1
		records := []Record{
			{[]byte("1"), []byte("Alice")},
			{[]byte("2"), []byte("Bob")},
		}

		// WHEN
		insert := NewInsert(trxId, nil, records)

		// THEN
		assert.NotNil(t, insert)
		assert.Nil(t, insert.table)
		assert.Equal(t, records, insert.records)
	})
}

func TestInsert_Next(t *testing.T) {
	t.Run("正常にレコードを挿入できる", func(t *testing.T) {
		initStorageManagerForTest(t)
		defer handler.Reset()

		tableName := "users"
		createTableForTest(t, tableName, []handler.CreateIndexParam{
			{Name: "name", ColName: "name", UkIdx: 1},
		}, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		// GIVEN
		var trxId handler.TrxId = 1
		records := []Record{
			{[]byte("1"), []byte("Alice")},
			{[]byte("2"), []byte("Bob")},
		}

		// テーブルアクセスメソッドを取得
		tbl, err := getTable(tableName)
		assert.NoError(t, err)

		// WHEN
		insert := NewInsert(trxId, tbl, records)
		_, err = insert.Next()

		// THEN
		assert.NoError(t, err)
		whileCondition := func(record Record) bool {
			return true
		}
		seqScan := NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			whileCondition,
		)
		res, err := fetchAll(seqScan)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(res))
		for i, record := range res {
			assert.Equal(t, records[i][0], record[0])
			assert.Equal(t, records[i][1], record[1])
		}
	})
}

func initStorageManagerForTest(t *testing.T) {
	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "10")
	handler.Reset()
	handler.Init()
}

func createTableForTest(t *testing.T, tableName string, indexes []handler.CreateIndexParam, columns []handler.CreateColumnParam) {
	createTable := NewCreateTable(tableName, 1, indexes, columns)
	_, err := createTable.Next()
	assert.NoError(t, err)
}

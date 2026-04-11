package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
	"minesql/internal/storage/lock"
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
		tbl, err := handler.Get().GetTable(tableName)
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
			0, lock.NewManager(5000), tbl,
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

	t.Run("INSERT で対象行に排他ロックが取得される", func(t *testing.T) {
		// GIVEN
		initLockTestHandler(t)
		defer handler.Reset()

		hdl := handler.Get()
		tbl := createLockTestTable(t)

		trx1 := hdl.BeginTrx()

		// WHEN: trx1 が INSERT (排他ロック取得)
		ins := NewInsert(trx1, tbl, []Record{
			{[]byte("a"), []byte("Alice")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)

		// THEN: trx2 が同じ行を UPDATE しようとするとタイムアウト
		trx2 := hdl.BeginTrx()
		upd := NewUpdate(trx2, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Updated")},
		}, NewTableScan(
			trx2, hdl.LockMgr, tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err = upd.Next()
		assert.ErrorIs(t, err, lock.ErrTimeout)

		assert.NoError(t, hdl.CommitTrx(trx1))
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

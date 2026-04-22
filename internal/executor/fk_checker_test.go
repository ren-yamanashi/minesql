package executor

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFKChecker_Insert(t *testing.T) {
	t.Run("参照先に値が存在する場合、INSERT が成功する", func(t *testing.T) {
		// GIVEN: 親テーブルと FK 付き子テーブルを作成
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		parentTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
			{Name: "name", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		childTable := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false}},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "user_id", Type: "string"},
			},
			[]handler.CreateConstraintParam{{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"}},
		)
		_, err = childTable.Next()
		assert.NoError(t, err)

		// 親テーブルにレコードを挿入
		usersTable, err := hdl.GetTable("users")
		assert.NoError(t, err)
		ins := NewInsert(trxId, usersTable, []Record{
			{[]byte("1"), []byte("Alice")},
		})
		_, err = ins.Next()
		assert.NoError(t, err)

		// WHEN: 子テーブルに参照先が存在する値で INSERT
		ordersTable, err := hdl.GetTable("orders")
		assert.NoError(t, err)
		insChild := NewInsert(trxId, ordersTable, []Record{
			{[]byte("100"), []byte("1")},
		})
		_, err = insChild.Next()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("参照先に値が存在しない場合、INSERT がエラーになる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		parentTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		childTable := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false}},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "user_id", Type: "string"},
			},
			[]handler.CreateConstraintParam{{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"}},
		)
		_, err = childTable.Next()
		assert.NoError(t, err)

		// WHEN: 親テーブルに値がない状態で子に INSERT
		ordersTable, err := hdl.GetTable("orders")
		assert.NoError(t, err)
		insChild := NewInsert(trxId, ordersTable, []Record{
			{[]byte("100"), []byte("999")},
		})
		_, err = insChild.Next()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "foreign key constraint fails")
	})
}

func TestFKChecker_Delete(t *testing.T) {
	t.Run("参照されていない親レコードは削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		parentTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
			{Name: "name", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		childTable := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false}},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "user_id", Type: "string"},
			},
			[]handler.CreateConstraintParam{{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"}},
		)
		_, err = childTable.Next()
		assert.NoError(t, err)

		// 親テーブルにレコードを挿入 (子テーブルからは参照されない)
		usersTable, err := hdl.GetTable("users")
		assert.NoError(t, err)
		ins := NewInsert(trxId, usersTable, []Record{
			{[]byte("1"), []byte("Alice")},
		})
		_, err = ins.Next()
		assert.NoError(t, err)

		// WHEN: 参照されていない親レコードを削除
		scanAll := testTableScan(usersTable, access.RecordSearchModeStart{}, func(Record) bool { return true })
		del := NewDelete(trxId, usersTable, scanAll)
		_, err = del.Next()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("参照されている親レコードは削除できない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		parentTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
			{Name: "name", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		childTable := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false}},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "user_id", Type: "string"},
			},
			[]handler.CreateConstraintParam{{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"}},
		)
		_, err = childTable.Next()
		assert.NoError(t, err)

		// 親にレコードを挿入し、子から参照
		usersTable, err := hdl.GetTable("users")
		assert.NoError(t, err)
		ins := NewInsert(trxId, usersTable, []Record{
			{[]byte("1"), []byte("Alice")},
		})
		_, err = ins.Next()
		assert.NoError(t, err)

		ordersTable, err := hdl.GetTable("orders")
		assert.NoError(t, err)
		insChild := NewInsert(trxId, ordersTable, []Record{
			{[]byte("100"), []byte("1")},
		})
		_, err = insChild.Next()
		assert.NoError(t, err)

		// WHEN: 参照されている親レコードを削除しようとする
		scanAll := testTableScan(usersTable, access.RecordSearchModeStart{}, func(Record) bool { return true })
		del := NewDelete(trxId, usersTable, scanAll)
		_, err = del.Next()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "foreign key constraint fails")
	})
}

func TestFKChecker_Update(t *testing.T) {
	t.Run("FK カラムの値を存在しない参照先に変更するとエラーになる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		parentTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		childTable := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false}},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "user_id", Type: "string"},
			},
			[]handler.CreateConstraintParam{{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"}},
		)
		_, err = childTable.Next()
		assert.NoError(t, err)

		// 親にレコード挿入、子から参照
		usersTable, err := hdl.GetTable("users")
		assert.NoError(t, err)
		ins := NewInsert(trxId, usersTable, []Record{
			{[]byte("1")},
		})
		_, err = ins.Next()
		assert.NoError(t, err)

		ordersTable, err := hdl.GetTable("orders")
		assert.NoError(t, err)
		insChild := NewInsert(trxId, ordersTable, []Record{
			{[]byte("100"), []byte("1")},
		})
		_, err = insChild.Next()
		assert.NoError(t, err)

		// WHEN: FK カラムを存在しない値に更新
		scanAll := testTableScan(ordersTable, access.RecordSearchModeStart{}, func(Record) bool { return true })
		upd := NewUpdate(trxId, ordersTable, []SetColumn{{Pos: 1, Value: []byte("999")}}, scanAll)
		_, err = upd.Next()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "foreign key constraint fails")
	})

	t.Run("参照されている親の PK を変更するとエラーになる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		parentTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
			{Name: "name", Type: "string"},
		}, nil)
		_, err := parentTable.Next()
		assert.NoError(t, err)

		childTable := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false}},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "user_id", Type: "string"},
			},
			[]handler.CreateConstraintParam{{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"}},
		)
		_, err = childTable.Next()
		assert.NoError(t, err)

		usersTable, err := hdl.GetTable("users")
		assert.NoError(t, err)
		ins := NewInsert(trxId, usersTable, []Record{
			{[]byte("1"), []byte("Alice")},
		})
		_, err = ins.Next()
		assert.NoError(t, err)

		ordersTable, err := hdl.GetTable("orders")
		assert.NoError(t, err)
		insChild := NewInsert(trxId, ordersTable, []Record{
			{[]byte("100"), []byte("1")},
		})
		_, err = insChild.Next()
		assert.NoError(t, err)

		// WHEN: 参照されている親の PK を変更
		scanAll := testTableScan(usersTable, access.RecordSearchModeStart{}, func(Record) bool { return true })
		upd := NewUpdate(trxId, usersTable, []SetColumn{{Pos: 0, Value: []byte("999")}}, scanAll)
		_, err = upd.Next()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "foreign key constraint fails")
	})
}

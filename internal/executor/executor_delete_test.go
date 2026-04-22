package executor

import (
	"testing"

	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"

	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	t.Run("正常に Delete Executor を生成できる", func(t *testing.T) {
		// GIVEN
		var trxId handler.TrxId = 1
		iterator := testTableScan(nil,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)

		// WHEN
		del := NewDelete(trxId, nil, iterator)

		// THEN
		assert.NotNil(t, del)
		assert.Nil(t, del.table)
		assert.NotNil(t, del.innerExecutor)
	})

	t.Run("SearchTable を使って全レコードを削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		del := NewDelete(trxId, tbl, testTableScan(tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err = del.Next()
		assert.NoError(t, err)

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: テーブルが空になっている
		scan := testTableScan(tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(results))
	})

	t.Run("条件付きで一部のレコードを削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1

		// テーブルアクセスメソッドを取得
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		// プライマリキーが "c" 未満のレコードを削除対象とする
		iterator := testTableScan(tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool {
				return string(record[0]) < "c"
			},
		)

		del := NewDelete(trxId, tbl, iterator)

		// WHEN
		_, err = del.Next()
		assert.NoError(t, err)

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: "c" 以降のレコードが残っている
		scan := testTableScan(tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(results))
		assert.Equal(t, []byte("c"), results[0][0])
		assert.Equal(t, []byte("d"), results[1][0])
		assert.Equal(t, []byte("e"), results[2][0])
	})

	t.Run("Filter を組み合わせて特定の条件のレコードを削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		// first_name が "Bob" のレコードを削除
		iterator := NewFilter(
			testTableScan(tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool {
				return string(record[1]) == "Bob"
			},
		)
		del := NewDelete(trxId, tbl, iterator)

		// WHEN
		_, err = del.Next()

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: "Bob" 以外のレコードが残っている
		scan := testTableScan(tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(results))
		for _, record := range results {
			assert.NotEqual(t, "Bob", string(record[1]))
		}
	})

	t.Run("削除後にユニークインデックスからも削除されている", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		InitStorageEngineForTest(t, tmpdir)
		defer handler.Reset()

		// テーブルアクセスメソッドを取得
		tbl, err := handler.Get().GetTable("users")
		assert.NoError(t, err)

		// インデックスアクセスメソッドを取得
		idx, err := tbl.GetSecondaryIndexByName("last_name")
		assert.NoError(t, err)

		// プライマリキーが "a" のレコードを削除 (last_name = "Doe")
		iterator := testTableScan(tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool {
				return string(record[0]) == "a"
			},
		)

		// Delete Executor を作成
		var trxId handler.TrxId = 1
		del := NewDelete(trxId, tbl, iterator)

		// WHEN
		_, err = del.Next()

		// THEN: 削除が成功する
		assert.NoError(t, err)

		// THEN: ユニークインデックスからも "Doe" が削除されている
		indexScan := NewIndexScan(
			tbl,
			idx,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		)
		results, err := fetchAll(indexScan)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(results))
		for _, record := range results {
			assert.NotEqual(t, "Doe", string(record[2]))
		}
	})

	t.Run("空のテーブルに対して削除しても正常に動作する", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManagerForTest(t)
		defer handler.Reset()
		_ = tmpdir

		var trxId handler.TrxId = 1
		createTableForTest(t, "empty_table", nil, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "value", Type: handler.ColumnTypeString},
		})

		tbl, err := handler.Get().GetTable("empty_table")
		assert.NoError(t, err)
		del := NewDelete(trxId, tbl, testTableScan(tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))

		// WHEN
		_, err = del.Next()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("FK 制約で参照されている親レコードは削除できない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		parentCt := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		}, nil)
		_, err := parentCt.Next()
		assert.NoError(t, err)

		childCt := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false}},
			[]handler.CreateColumnParam{
				{Name: "id", Type: handler.ColumnTypeString},
				{Name: "user_id", Type: handler.ColumnTypeString},
			},
			[]handler.CreateConstraintParam{{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"}},
		)
		_, err = childCt.Next()
		assert.NoError(t, err)

		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		// 親にレコード挿入、子から参照
		usersTbl, err := hdl.GetTable("users")
		assert.NoError(t, err)
		ins := NewInsert(trxId, usersTbl, []Record{{[]byte("1"), []byte("Alice")}})
		_, err = ins.Next()
		assert.NoError(t, err)

		ordersTbl, err := hdl.GetTable("orders")
		assert.NoError(t, err)
		insChild := NewInsert(trxId, ordersTbl, []Record{{[]byte("100"), []byte("1")}})
		_, err = insChild.Next()
		assert.NoError(t, err)

		// WHEN: 参照されている親レコードを削除しようとする
		del := NewDelete(trxId, usersTbl, testTableScan(usersTbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))
		_, err = del.Next()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "foreign key constraint fails")
	})

	t.Run("FK 制約で参照されていない親レコードは削除できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		defer handler.Reset()

		parentCt := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
		}, nil)
		_, err := parentCt.Next()
		assert.NoError(t, err)

		childCt := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false}},
			[]handler.CreateColumnParam{
				{Name: "id", Type: handler.ColumnTypeString},
				{Name: "user_id", Type: handler.ColumnTypeString},
			},
			[]handler.CreateConstraintParam{{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"}},
		)
		_, err = childCt.Next()
		assert.NoError(t, err)

		hdl := handler.Get()
		trxId := hdl.BeginTrx()

		// 親にのみレコード挿入 (子からは参照されない)
		usersTbl, err := hdl.GetTable("users")
		assert.NoError(t, err)
		ins := NewInsert(trxId, usersTbl, []Record{{[]byte("1")}})
		_, err = ins.Next()
		assert.NoError(t, err)

		// WHEN: 参照されていない親レコードを削除
		del := NewDelete(trxId, usersTbl, testTableScan(usersTbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))
		_, err = del.Next()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("DELETE 済みの行は他のトランザクションの scan でスキップされる", func(t *testing.T) {
		// GIVEN
		initLockTestHandler(t)
		defer handler.Reset()

		hdl := handler.Get()
		tbl := createLockTestTable(t)
		insertLockTestData(t, tbl)

		trx1 := hdl.BeginTrx()

		// trx1 が row "a" を DELETE (排他ロック取得 + DeleteMark 設定)
		del1 := NewDelete(trx1, tbl, testTableScan(tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err := del1.Next()
		assert.NoError(t, err)

		// WHEN: trx2 が同じ行を DELETE しようとする
		// DeleteMark=1 の行は scan でスキップされるため、削除対象が 0 件で正常終了する
		trx2 := hdl.BeginTrx()
		del2 := NewDelete(trx2, tbl, testTableScan(tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err = del2.Next()

		// THEN: エラーなし (削除対象がないので何もしない)
		assert.NoError(t, err)

		assert.NoError(t, hdl.CommitTrx(trx1))
		assert.NoError(t, hdl.CommitTrx(trx2))
	})
}

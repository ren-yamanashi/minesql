package planner

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/ast"
	"github.com/ren-yamanashi/minesql/internal/executor"
	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/handler"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

func TestPlanUpdate(t *testing.T) {
	t.Run("単一カラムの更新で Update Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("Jane")},
			},
			Where: nil,
		}

		// WHEN
		exec, err := PlanUpdate(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Update{}, exec)
	})

	t.Run("複数カラムの更新が正しく実行される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tbl := getPlannerTable(t, "users")
		hdl := handler.Get()
		err := tbl.Insert(hdl.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("1"), []byte("John"), []byte("Smith")})
		assert.NoError(t, err)

		var trxId handler.TrxId = 1
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("Jane")},
				{Column: *ast.NewColumnId("last_name"), Value: ast.NewStringLiteral("Doe")},
			},
			Where: nil,
		}
		exec, err := PlanUpdate(trxId, stmt)
		assert.NoError(t, err)

		// WHEN
		_, err = exec.Next()
		assert.NoError(t, err)

		// THEN: 更新後のレコードが正しい
		iter, err := tbl.Search(hdl.BufferPool, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "1", string(record[0]))
		assert.Equal(t, "Jane", string(record[1]))
		assert.Equal(t, "Doe", string(record[2]))
	})

	t.Run("存在しないテーブル名の場合、エラーが返る", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("nonexistent"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("id"), Value: ast.NewStringLiteral("1")},
			},
		}

		// WHEN
		exec, err := PlanUpdate(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
	})

	t.Run("存在しないカラム名の場合、エラーが返る", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("nonexistent"), Value: ast.NewStringLiteral("val")},
			},
			Where: nil,
		}

		// WHEN
		exec, err := PlanUpdate(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "column does not exist: nonexistent")
	})

	t.Run("WHERE句に存在しないカラムが指定された場合、エラーが返る", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("Jane")},
			},
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("non_existent")),
					ast.NewRhsLiteral(ast.NewStringLiteral("test")),
				),
			},
		}

		// WHEN
		exec, err := PlanUpdate(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "non_existent")
	})

	t.Run("生成された Executor でレコードが正しく更新される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		hdl := handler.Get()
		tbl := getPlannerTable(t, "users")

		// データを挿入
		err := tbl.Insert(hdl.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = tbl.Insert(hdl.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// "a" の first_name を "Jane" に更新する
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("Jane")},
			},
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("a")),
				),
			},
		}

		// WHEN
		exec, err := PlanUpdate(trxId, stmt)
		assert.NoError(t, err)
		_, err = exec.Next()
		assert.NoError(t, err)

		// THEN: "a" の first_name が "Jane" に更新されている
		scan := executor.NewTableScan(executor.TableScanParams{
			ReadView:       access.NewReadView(0, nil, ^uint64(0)),
			VersionReader:  access.NewVersionReader(nil),
			Table:          tbl,
			SearchMode:     access.RecordSearchModeStart{},
			WhileCondition: func(record executor.Record) bool { return true },
		})
		results := fetchAll(t, scan)
		assert.Equal(t, 2, len(results))
		assert.Equal(t, executor.Record{[]byte("a"), []byte("Jane"), []byte("Doe")}, results[0])
		assert.Equal(t, executor.Record{[]byte("b"), []byte("Alice"), []byte("Smith")}, results[1])
	})

	t.Run("PlanStart 経由で Update Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("Jane")},
			},
			Where: nil,
		}

		// WHEN
		plan, err := Start(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		assert.IsType(t, &executor.Update{}, plan.Exec)
	})

	t.Run("PlanStart 経由で WHERE 句付きの Update Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("Jane")},
			},
			Where: &ast.WhereClause{Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("last_name")),
				ast.NewRhsLiteral(ast.NewStringLiteral("Doe")),
			)},
		}

		// WHEN
		plan, err := Start(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		assert.IsType(t, &executor.Update{}, plan.Exec)
	})

	t.Run("SetClauses が空の場合でも Executor が生成され、レコードは変更されない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		tbl := getPlannerTable(t, "users")
		hdl := handler.Get()
		err := tbl.Insert(hdl.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("1"), []byte("John"), []byte("Smith")})
		assert.NoError(t, err)

		var trxId handler.TrxId = 1
		stmt := &ast.UpdateStmt{
			Table:      *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{},
			Where:      nil,
		}
		exec, err := PlanUpdate(trxId, stmt)
		assert.NoError(t, err)

		// WHEN
		_, err = exec.Next()
		assert.NoError(t, err)

		// THEN: レコードは変更されていない
		iter, err := tbl.Search(hdl.BufferPool, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "1", string(record[0]))
		assert.Equal(t, "John", string(record[1]))
		assert.Equal(t, "Smith", string(record[2]))
	})
	t.Run("Current Read: 自トランザクション開始後にコミットされた行も UPDATE 対象になる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer handler.Reset()

		hdl := handler.Get()
		tbl := getPlannerTable(t, "users")

		// T1 を開始 (まだ何もしない)
		trx1 := hdl.BeginTrx()

		// T2 が行を INSERT してコミット (T1 の開始後)
		trx2 := hdl.BeginTrx()
		err := tbl.Insert(hdl.BufferPool, trx2, hdl.LockMgr, [][]byte{[]byte("1"), []byte("Alice"), []byte("Doe")})
		assert.NoError(t, err)
		assert.NoError(t, hdl.CommitTrx(trx2))

		// WHEN: T1 が UPDATE を実行 (Current Read なので T2 のコミット済み行が見える)
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("Bob")},
			},
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("1")),
				),
			},
		}
		exec, err := PlanUpdate(trx1, stmt)
		assert.NoError(t, err)
		_, err = exec.Next()
		assert.NoError(t, err)

		// THEN: 行が更新されている
		iter, err := tbl.Search(hdl.BufferPool, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Bob", string(record[1]))
	})
}

//nolint:unparam // テーブル名は将来的に変わりうる
func getPlannerTable(t *testing.T, tableName string) *access.Table {
	t.Helper()
	tbl, err := handler.Get().GetTable(tableName)
	assert.NoError(t, err)
	return tbl
}

package planner

import (
	"minesql/internal/ast"
	"minesql/internal/executor"
	"minesql/internal/storage/access"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPlanDelete(t *testing.T) {
	t.Run("存在しないテーブル名の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		stmt := &ast.DeleteStmt{
			From: *ast.NewTableId("nonexistent"),
		}

		// WHEN
		exec, err := PlanDelete(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "nonexistent")
	})

	t.Run("存在するテーブル名の場合、Delete Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, nil)

		stmt := &ast.DeleteStmt{
			From:  *ast.NewTableId("users"),
			Where: nil,
		}

		// WHEN
		exec, err := PlanDelete(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Delete{}, exec)
	})

	t.Run("WHERE句に条件が指定されている場合、Delete Executorが生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})

		stmt := &ast.DeleteStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("1")),
				),
			},
		}

		// WHEN
		exec, err := PlanDelete(trxId, stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Delete{}, exec)
	})

	t.Run("WHERE句に存在しないカラムが指定された場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		var trxId handler.TrxId = 1
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
		})

		stmt := &ast.DeleteStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("non_existent")),
					ast.NewRhsLiteral(ast.NewStringLiteral("test")),
				),
			},
		}

		// WHEN
		exec, err := PlanDelete(trxId, stmt)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "non_existent")
	})

	t.Run("Current Read: 自トランザクション開始後にコミットされた行も DELETE 対象になる", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer handler.Reset()

		hdl := handler.Get()
		createTableForTest(t, []handler.CreateColumnParam{
			{Name: "id", Type: handler.ColumnTypeString},
			{Name: "name", Type: handler.ColumnTypeString},
		})
		tbl, err := hdl.GetTable("users")
		assert.NoError(t, err)

		// T1 を開始 (まだ何もしない)
		trx1 := hdl.BeginTrx()

		// T2 が行を INSERT してコミット (T1 の開始後)
		trx2 := hdl.BeginTrx()
		err = tbl.Insert(hdl.BufferPool, trx2, hdl.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, hdl.CommitTrx(trx2))

		// WHEN: T1 が DELETE を実行 (Current Read なので T2 のコミット済み行が見える)
		stmt := &ast.DeleteStmt{
			From: *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("1")),
				),
			},
		}
		exec, err := PlanDelete(trx1, stmt)
		assert.NoError(t, err)
		_, err = exec.Next()
		assert.NoError(t, err)

		// THEN: 行が削除されている (DeleteMark=1 なので全可視スキャンでも返らない)
		iter, err := tbl.Search(hdl.BufferPool, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

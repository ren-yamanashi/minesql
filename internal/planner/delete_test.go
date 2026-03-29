package planner

import (
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/undo"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelete_Build(t *testing.T) {
	t.Run("存在しないテーブル名の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()
		var trxId undo.TrxId = 1
		stmt := &ast.DeleteStmt{
			StmtType: ast.StmtTypeDelete,
			From:     *ast.NewTableId("nonexistent"),
		}
		planner := NewDelete(stmt)

		// WHEN
		exec, err := planner.Build(undoLog, trxId)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "nonexistent")
	})

	t.Run("存在するテーブル名の場合、Delete Executor が生成される", func(t *testing.T) {
		// GIVEN
		initStorageManagerForTest(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()
		var trxId undo.TrxId = 1
		createTableForTest(t, nil)

		stmt := &ast.DeleteStmt{
			StmtType: ast.StmtTypeDelete,
			From:     *ast.NewTableId("users"),
			Where:    &ast.WhereClause{IsSet: false},
		}
		planner := NewDelete(stmt)

		// WHEN
		exec, err := planner.Build(undoLog, trxId)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Delete{}, exec)
	})
}

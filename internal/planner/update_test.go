package planner

import (
	"minesql/internal/access"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewUpdatePlanner(t *testing.T) {
	t.Run("正常に UpdatePlanner が生成される", func(t *testing.T) {
		// GIVEN
		stmt := &statement.UpdateStmt{
			Table: *identifier.NewTableId("users"),
			SetClauses: []*statement.SetClause{
				{Column: *identifier.NewColumnId("first_name"), Value: literal.NewStringLiteral("'Jane'", "Jane")},
			},
		}
		iterator := executor.NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)

		// WHEN
		planner := NewUpdatePlanner(stmt, iterator)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
		assert.NotNil(t, planner.Iterator)
	})
}

func TestUpdatePlanner_Next(t *testing.T) {
	t.Run("単一カラムの更新で Update Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &statement.UpdateStmt{
			Table: *identifier.NewTableId("users"),
			SetClauses: []*statement.SetClause{
				{Column: *identifier.NewColumnId("first_name"), Value: literal.NewStringLiteral("'Jane'", "Jane")},
			},
		}
		iterator := executor.NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdatePlanner(stmt, iterator)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Update{}, exec)
	})

	t.Run("複数カラムの更新で SetColumns のカラム位置と値が正しく変換される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &statement.UpdateStmt{
			Table: *identifier.NewTableId("users"),
			SetClauses: []*statement.SetClause{
				{Column: *identifier.NewColumnId("first_name"), Value: literal.NewStringLiteral("'Jane'", "Jane")},
				{Column: *identifier.NewColumnId("last_name"), Value: literal.NewStringLiteral("'Doe'", "Doe")},
			},
		}
		iterator := executor.NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdatePlanner(stmt, iterator)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		upd, ok := exec.(*executor.Update)
		assert.True(t, ok)
		assert.Equal(t, 2, len(upd.SetColumns))
		assert.Equal(t, uint16(1), upd.SetColumns[0].Pos) // first_name の位置
		assert.Equal(t, []byte("Jane"), upd.SetColumns[0].Value)
		assert.Equal(t, uint16(2), upd.SetColumns[1].Pos) // last_name の位置
		assert.Equal(t, []byte("Doe"), upd.SetColumns[1].Value)
	})

	t.Run("存在しないカラム名の場合、エラーが返る", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &statement.UpdateStmt{
			Table: *identifier.NewTableId("users"),
			SetClauses: []*statement.SetClause{
				{Column: *identifier.NewColumnId("nonexistent"), Value: literal.NewStringLiteral("'val'", "val")},
			},
		}
		iterator := executor.NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdatePlanner(stmt, iterator)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
		assert.Contains(t, err.Error(), "column does not exist: nonexistent")
	})

	t.Run("存在しないテーブル名の場合、エラーが返る", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &statement.UpdateStmt{
			Table: *identifier.NewTableId("nonexistent"),
			SetClauses: []*statement.SetClause{
				{Column: *identifier.NewColumnId("id"), Value: literal.NewStringLiteral("'1'", "1")},
			},
		}
		iterator := executor.NewSearchTable(
			"nonexistent",
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdatePlanner(stmt, iterator)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
	})

	t.Run("生成された Executor でレコードが正しく更新される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		sm := engine.Get()
		tblMeta, err := sm.Catalog.GetTableMetadataByName("users")
		assert.NoError(t, err)
		tbl, err := tblMeta.GetTable()
		assert.NoError(t, err)

		// データを挿入
		err = tbl.Insert(sm.BufferPool, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = tbl.Insert(sm.BufferPool, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// "a" の first_name を "Jane" に更新する UpdatePlanner を作成
		stmt := &statement.UpdateStmt{
			Table: *identifier.NewTableId("users"),
			SetClauses: []*statement.SetClause{
				{Column: *identifier.NewColumnId("first_name"), Value: literal.NewStringLiteral("'Jane'", "Jane")},
			},
		}
		iterator := executor.NewSearchTable(
			"users",
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record executor.Record) bool {
				return string(record[0]) == "a"
			},
		)
		planner := NewUpdatePlanner(stmt, iterator)

		// WHEN
		exec, err := planner.Next()
		assert.NoError(t, err)
		err = exec.Execute()
		assert.NoError(t, err)

		// THEN: "a" の first_name が "Jane" に更新されている
		scan := executor.NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		results, err := executor.FetchAll(scan)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(results))
		assert.Equal(t, executor.Record{[]byte("a"), []byte("Jane"), []byte("Doe")}, results[0])
		assert.Equal(t, executor.Record{[]byte("b"), []byte("Alice"), []byte("Smith")}, results[1])
	})

	t.Run("PlanStart 経由で Update Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &statement.UpdateStmt{
			Table: *identifier.NewTableId("users"),
			SetClauses: []*statement.SetClause{
				{Column: *identifier.NewColumnId("first_name"), Value: literal.NewStringLiteral("'Jane'", "Jane")},
			},
			Where: nil,
		}

		// WHEN
		exec, err := PlanStart(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Update{}, exec)
	})

	t.Run("PlanStart 経由で WHERE 句付きの Update Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &statement.UpdateStmt{
			Table: *identifier.NewTableId("users"),
			SetClauses: []*statement.SetClause{
				{Column: *identifier.NewColumnId("first_name"), Value: literal.NewStringLiteral("'Jane'", "Jane")},
			},
			Where: statement.NewWhereClause(
				expression.NewBinaryExpr(
					"=",
					expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
					expression.NewRhsLiteral(literal.NewStringLiteral("'Doe'", "Doe")),
				),
			),
		}

		// WHEN
		exec, err := PlanStart(stmt)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		assert.IsType(t, &executor.Update{}, exec)
	})

	t.Run("SetClauses が空の場合、空の SetColumns で Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &statement.UpdateStmt{
			Table:      *identifier.NewTableId("users"),
			SetClauses: []*statement.SetClause{},
		}
		iterator := executor.NewSearchTable(
			"users",
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdatePlanner(stmt, iterator)

		// WHEN
		exec, err := planner.Next()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		upd, ok := exec.(*executor.Update)
		assert.True(t, ok)
		assert.Empty(t, upd.SetColumns)
	})
}

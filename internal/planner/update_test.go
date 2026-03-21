package planner

import (
	"minesql/internal/access"
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewUpdate(t *testing.T) {
	t.Run("正常に Update が生成される", func(t *testing.T) {
		// GIVEN
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("'Jane'", "Jane")},
			},
		}
		iterator := executor.NewTableScan(
			nil,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)

		// WHEN
		planner := NewUpdate(stmt, iterator)

		// THEN
		assert.NotNil(t, planner)
		assert.Equal(t, stmt, planner.Stmt)
		assert.NotNil(t, planner.Iterator)
	})
}

func TestUpdate_Build(t *testing.T) {
	t.Run("単一カラムの更新で Update Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tbl := getPlannerTableAccessMethod(t, "users")

		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("'Jane'", "Jane")},
			},
		}
		iterator := executor.NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdate(stmt, iterator)

		// WHEN
		exec, err := planner.Build()

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

		tbl := getPlannerTableAccessMethod(t, "users")

		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("'Jane'", "Jane")},
				{Column: *ast.NewColumnId("last_name"), Value: ast.NewStringLiteral("'Doe'", "Doe")},
			},
		}
		iterator := executor.NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdate(stmt, iterator)

		// WHEN
		exec, err := planner.Build()

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

	t.Run("存在しないテーブル名の場合、エラーが返る", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("nonexistent"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("id"), Value: ast.NewStringLiteral("'1'", "1")},
			},
		}
		iterator := executor.NewTableScan(
			nil,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdate(stmt, iterator)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
	})

	t.Run("存在しないカラム名の場合、エラーが返る", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		tbl := getPlannerTableAccessMethod(t, "users")

		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("nonexistent"), Value: ast.NewStringLiteral("'val'", "val")},
			},
		}
		iterator := executor.NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdate(stmt, iterator)

		// WHEN
		exec, err := planner.Build()

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

		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("nonexistent"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("id"), Value: ast.NewStringLiteral("'1'", "1")},
			},
		}
		iterator := executor.NewTableScan(
			nil,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdate(stmt, iterator)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.Error(t, err)
		assert.Nil(t, exec)
	})

	t.Run("生成された Executor でレコードが正しく更新される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		e := engine.Get()
		tbl := getPlannerTableAccessMethod(t, "users")

		// データを挿入
		err := tbl.Insert(e.BufferPool, [][]byte{[]byte("a"), []byte("John"), []byte("Doe")})
		assert.NoError(t, err)
		err = tbl.Insert(e.BufferPool, [][]byte{[]byte("b"), []byte("Alice"), []byte("Smith")})
		assert.NoError(t, err)

		// "a" の first_name を "Jane" に更新する UpdatePlanner を作成
		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("'Jane'", "Jane")},
			},
		}
		iterator := executor.NewTableScan(
			tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record executor.Record) bool {
				return string(record[0]) == "a"
			},
		)
		planner := NewUpdate(stmt, iterator)

		// WHEN
		exec, err := planner.Build()
		assert.NoError(t, err)
		_, err = exec.Next()
		assert.NoError(t, err)

		// THEN: "a" の first_name が "Jane" に更新されている
		scan := executor.NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		results := fetchAll(t, scan)
		assert.Equal(t, 2, len(results))
		assert.Equal(t, executor.Record{[]byte("a"), []byte("Jane"), []byte("Doe")}, results[0])
		assert.Equal(t, executor.Record{[]byte("b"), []byte("Alice"), []byte("Smith")}, results[1])
	})

	t.Run("PlanStart 経由で Update Executor が生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		initStorageManager(t, tmpdir)
		defer engine.Reset()

		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("'Jane'", "Jane")},
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

		stmt := &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("first_name"), Value: ast.NewStringLiteral("'Jane'", "Jane")},
			},
			Where: &ast.WhereClause{Condition: ast.NewBinaryExpr(
				"=",
				ast.NewLhsColumn(*ast.NewColumnId("last_name")),
				ast.NewRhsLiteral(ast.NewStringLiteral("'Doe'", "Doe")),
			), IsSet: true},
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

		tbl := getPlannerTableAccessMethod(t, "users")

		stmt := &ast.UpdateStmt{
			Table:      *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{},
		}
		iterator := executor.NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		)
		planner := NewUpdate(stmt, iterator)

		// WHEN
		exec, err := planner.Build()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, exec)
		upd, ok := exec.(*executor.Update)
		assert.True(t, ok)
		assert.Empty(t, upd.SetColumns)
	})
}

func getPlannerTableAccessMethod(t *testing.T, tableName string) *access.TableAccessMethod {
	t.Helper()
	e := engine.Get()
	tblMeta, ok := e.Catalog.GetTableMetadataByName(tableName)
	if !ok {
		t.Fatalf("table %s not found in catalog", tableName)
	}
	tbl, err := tblMeta.GetTable()
	assert.NoError(t, err)
	return tbl
}

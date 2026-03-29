package planner

import (
	"fmt"
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/undo"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Executor から全レコードを取得する
func fetchAll(t *testing.T, iter executor.Executor) []executor.Record {
	t.Helper()
	var records []executor.Record
	for {
		record, err := iter.Next()
		assert.NoError(t, err)
		if record == nil {
			return records
		}
		records = append(records, record)
	}
}

// AST を直接構築 → PlanStart → ExecutePlan で実行する
func executePlan(t *testing.T, undoLog *undo.UndoLog, stmt ast.Statement) []executor.Record {
	t.Helper()
	var trxId undo.TrxId = 1
	exec, err := Start(undoLog, trxId, stmt)
	assert.NoError(t, err)

	return fetchAll(t, exec)
}

// ストレージを初期化し、5 カラムの users テーブルを作成してデータを投入する
func setupUsersTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	undoLog := undo.NewUndoLog()

	// CREATE TABLE
	executePlan(t, undoLog, &ast.CreateTableStmt{
		StmtType:  ast.StmtTypeCreate,
		Keyword:   ast.KeywordTable,
		TableName: "users",
		CreateDefinitions: []ast.Definition{
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "first_name", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "last_name", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "gender", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "username", DataType: ast.DataTypeVarchar},
			&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
				*ast.NewColumnId("id"),
			}},
			&ast.ConstraintUniqueKeyDef{DefType: ast.DefTypeConstraintUniqueKey, Column: *ast.NewColumnId("username")},
		},
	})

	// INSERT
	executePlan(t, undoLog, &ast.InsertStmt{
		StmtType: ast.StmtTypeInsert,
		Table:    *ast.NewTableId("users"),
		Cols: []ast.ColumnId{
			*ast.NewColumnId("id"),
			*ast.NewColumnId("first_name"),
			*ast.NewColumnId("last_name"),
			*ast.NewColumnId("gender"),
			*ast.NewColumnId("username"),
		},
		Values: [][]ast.Literal{
			{
				ast.NewStringLiteral("1", "1"),
				ast.NewStringLiteral("John", "John"),
				ast.NewStringLiteral("Doe", "Doe"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("johndoe", "johndoe"),
			},
			{
				ast.NewStringLiteral("2", "2"),
				ast.NewStringLiteral("John", "John"),
				ast.NewStringLiteral("Doe2", "Doe2"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("johndoe2", "johndoe2"),
			},
			{
				ast.NewStringLiteral("3", "3"),
				ast.NewStringLiteral("John", "John"),
				ast.NewStringLiteral("Doe3", "Doe3"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("johndoe3", "johndoe3"),
			},
			{
				ast.NewStringLiteral("4", "4"),
				ast.NewStringLiteral("Jane", "Jane"),
				ast.NewStringLiteral("Doe2", "Doe2"),
				ast.NewStringLiteral("female", "female"),
				ast.NewStringLiteral("janedoe", "janedoe"),
			},
			{
				ast.NewStringLiteral("5", "5"),
				ast.NewStringLiteral("Jonathan", "Jonathan"),
				ast.NewStringLiteral("Black", "Black"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("jonathanblack", "jonathanblack"),
			},
			{
				ast.NewStringLiteral("6", "6"),
				ast.NewStringLiteral("Tom", "Tom"),
				ast.NewStringLiteral("Brown", "Brown"),
				ast.NewStringLiteral("male", "male"),
				ast.NewStringLiteral("tombrown", "tombrown"),
			},
		},
	})
}

// レコードを strings.Builder に書き出す
func writeRecords(sb *strings.Builder, records []executor.Record) {
	for _, record := range records {
		cols := make([]string, len(record))
		for i, col := range record {
			cols[i] = string(col)
		}
		fmt.Fprintf(sb, "  (%s)\n", strings.Join(cols, ", "))
	}
	fmt.Fprintf(sb, "  合計: %d 件\n", len(records))
}

func TestPlannerIntegration(t *testing.T) {
	t.Run("SELECT でフルテーブルスキャンできる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()

		// WHEN
		records := executePlan(t, undoLog, &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where:    &ast.WhereClause{IsSet: false},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== SELECT 全件 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== SELECT 全件 ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 6 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("WHERE 句で等値検索できる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()

		// WHEN
		records := executePlan(t, undoLog, &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("username")),
					ast.NewRhsLiteral(ast.NewStringLiteral("janedoe", "janedoe")),
				),
				IsSet: true,
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE 等値検索 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE 等値検索 ===\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  合計: 1 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("AND と OR の複合条件でフィルタリングできる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()

		// WHEN: (first_name < 'K' AND gender = 'male' AND last_name >= 'Doe') OR first_name = 'Tom'
		records := executePlan(t, undoLog, &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"OR",
					ast.NewLhsExpr(
						ast.NewBinaryExpr(
							"AND",
							ast.NewLhsExpr(
								ast.NewBinaryExpr(
									"<",
									ast.NewLhsColumn(*ast.NewColumnId("first_name")),
									ast.NewRhsLiteral(ast.NewStringLiteral("K", "K")),
								),
							),
							ast.NewRhsExpr(
								ast.NewBinaryExpr(
									"AND",
									ast.NewLhsExpr(
										ast.NewBinaryExpr(
											"=",
											ast.NewLhsColumn(*ast.NewColumnId("gender")),
											ast.NewRhsLiteral(ast.NewStringLiteral("male", "male")),
										),
									),
									ast.NewRhsExpr(
										ast.NewBinaryExpr(
											">=",
											ast.NewLhsColumn(*ast.NewColumnId("last_name")),
											ast.NewRhsLiteral(ast.NewStringLiteral("Doe", "Doe")),
										),
									),
								),
							),
						),
					),
					ast.NewRhsExpr(
						ast.NewBinaryExpr(
							"=",
							ast.NewLhsColumn(*ast.NewColumnId("first_name")),
							ast.NewRhsLiteral(ast.NewStringLiteral("Tom", "Tom")),
						),
					),
				),
				IsSet: true,
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== AND/OR 複合条件 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== AND/OR 複合条件 ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 4 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("PK に対する >= 条件で複数行が返される", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()

		// WHEN: WHERE id >= '4' → id=4, 5, 6 の 3 件が返されるべき
		records := executePlan(t, undoLog, &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					">=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("4", "4")),
				),
				IsSet: true,
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE id >= '4' ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE id >= '4' ===\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 3 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("PK に対する > 条件で複数行が返される", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()

		// WHEN: WHERE id > '4' → id=5, 6 の 2 件が返されるべき
		records := executePlan(t, undoLog, &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					">",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("4", "4")),
				),
				IsSet: true,
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE id > '4' ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE id > '4' ===\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 2 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("PK に対する <= 条件で複数行が返される", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()

		// WHEN: WHERE id <= '3' → id=1, 2, 3 の 3 件が返されるべき
		records := executePlan(t, undoLog, &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"<=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("3", "3")),
				),
				IsSet: true,
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE id <= '3' ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE id <= '3' ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  合計: 3 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("PK に対する < 条件で複数行が返される", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()

		// WHEN: WHERE id < '3' → id=1, 2 の 2 件が返されるべき
		records := executePlan(t, undoLog, &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"<",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("3", "3")),
				),
				IsSet: true,
			},
		})

		// THEN
		var sb strings.Builder
		sb.WriteString("=== WHERE id < '3' ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== WHERE id < '3' ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  合計: 2 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("UPDATE でレコードを更新できる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()

		// WHEN (UPDATE users SET last_name = 'Smith' WHERE username = 'johndoe')
		executePlan(t, undoLog, &ast.UpdateStmt{
			Table: *ast.NewTableId("users"),
			SetClauses: []*ast.SetClause{
				{Column: *ast.NewColumnId("last_name"), Value: ast.NewStringLiteral("'Smith'", "Smith")},
			},
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("username")),
					ast.NewRhsLiteral(ast.NewStringLiteral("johndoe", "johndoe")),
				),
				IsSet: true,
			},
		})

		// THEN
		records := executePlan(t, undoLog, &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where:    &ast.WhereClause{IsSet: false},
		})

		var sb strings.Builder
		sb.WriteString("=== UPDATE 後の全件 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== UPDATE 後の全件 ===\n" +
			"  (1, John, Smith, male, johndoe)\n" +
			"  (2, John, Doe2, male, johndoe2)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 6 件\n"
		assert.Equal(t, expected, sb.String())
	})

	t.Run("DELETE でレコードを削除できる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		undoLog := undo.NewUndoLog()

		// WHEN
		executePlan(t, undoLog, &ast.DeleteStmt{
			StmtType: ast.StmtTypeDelete,
			From:     *ast.NewTableId("users"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("username")),
					ast.NewRhsLiteral(ast.NewStringLiteral("johndoe2", "johndoe2")),
				),
				IsSet: true,
			},
		})

		// THEN
		records := executePlan(t, undoLog, &ast.SelectStmt{
			StmtType: ast.StmtTypeSelect,
			From:     *ast.NewTableId("users"),
			Where:    &ast.WhereClause{IsSet: false},
		})

		var sb strings.Builder
		sb.WriteString("=== DELETE 後の全件 ===\n")
		writeRecords(&sb, records)

		expected := "" +
			"=== DELETE 後の全件 ===\n" +
			"  (1, John, Doe, male, johndoe)\n" +
			"  (3, John, Doe3, male, johndoe3)\n" +
			"  (4, Jane, Doe2, female, janedoe)\n" +
			"  (5, Jonathan, Black, male, jonathanblack)\n" +
			"  (6, Tom, Brown, male, tombrown)\n" +
			"  合計: 5 件\n"
		assert.Equal(t, expected, sb.String())
	})
}

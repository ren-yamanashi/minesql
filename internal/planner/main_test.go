package planner

import (
	"fmt"
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/planner/ast/definition"
	"minesql/internal/planner/ast/expression"
	"minesql/internal/planner/ast/identifier"
	"minesql/internal/planner/ast/literal"
	"minesql/internal/planner/ast/statement"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// AST を直接構築 → PlanStart → ExecutePlan で実行する
func executePlan(t *testing.T, stmt statement.Statement) []executor.Record {
	t.Helper()
	exec, err := PlanStart(stmt)
	assert.NoError(t, err)
	records, err := executor.ExecutePlan(exec)
	assert.NoError(t, err)
	return records
}

// ストレージを初期化し、5 カラムの users テーブルを作成してデータを投入する
func setupUsersTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	// CREATE TABLE
	executePlan(t, statement.NewCreateTableStmt(
		"users",
		[]definition.Definition{
			definition.NewColumnDef("id", definition.DataTypeVarchar),
			definition.NewColumnDef("first_name", definition.DataTypeVarchar),
			definition.NewColumnDef("last_name", definition.DataTypeVarchar),
			definition.NewColumnDef("gender", definition.DataTypeVarchar),
			definition.NewColumnDef("username", definition.DataTypeVarchar),
			definition.NewConstraintPrimaryKeyDef([]identifier.ColumnId{
				*identifier.NewColumnId("id"),
			}),
			definition.NewConstraintUniqueKeyDef(*identifier.NewColumnId("username")),
		},
	))

	// INSERT
	executePlan(t, statement.NewInsertStmt(
		*identifier.NewTableId("users"),
		[]identifier.ColumnId{
			*identifier.NewColumnId("id"),
			*identifier.NewColumnId("first_name"),
			*identifier.NewColumnId("last_name"),
			*identifier.NewColumnId("gender"),
			*identifier.NewColumnId("username"),
		},
		[][]literal.Literal{
			{
				literal.NewStringLiteral("1", "1"),
				literal.NewStringLiteral("John", "John"),
				literal.NewStringLiteral("Doe", "Doe"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("johndoe", "johndoe"),
			},
			{
				literal.NewStringLiteral("2", "2"),
				literal.NewStringLiteral("John", "John"),
				literal.NewStringLiteral("Doe2", "Doe2"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("johndoe2", "johndoe2"),
			},
			{
				literal.NewStringLiteral("3", "3"),
				literal.NewStringLiteral("John", "John"),
				literal.NewStringLiteral("Doe3", "Doe3"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("johndoe3", "johndoe3"),
			},
			{
				literal.NewStringLiteral("4", "4"),
				literal.NewStringLiteral("Jane", "Jane"),
				literal.NewStringLiteral("Doe2", "Doe2"),
				literal.NewStringLiteral("female", "female"),
				literal.NewStringLiteral("janedoe", "janedoe"),
			},
			{
				literal.NewStringLiteral("5", "5"),
				literal.NewStringLiteral("Jonathan", "Jonathan"),
				literal.NewStringLiteral("Black", "Black"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("jonathanblack", "jonathanblack"),
			},
			{
				literal.NewStringLiteral("6", "6"),
				literal.NewStringLiteral("Tom", "Tom"),
				literal.NewStringLiteral("Brown", "Brown"),
				literal.NewStringLiteral("male", "male"),
				literal.NewStringLiteral("tombrown", "tombrown"),
			},
		},
	))
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

		// WHEN
		records := executePlan(t, statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			nil,
		))

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

		// WHEN
		records := executePlan(t, statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			statement.NewWhereClause(
				expression.NewBinaryExpr(
					"=",
					expression.NewLhsColumn(*identifier.NewColumnId("username")),
					expression.NewRhsLiteral(literal.NewStringLiteral("janedoe", "janedoe")),
				),
			),
		))

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

		// WHEN: (first_name < 'K' AND gender = 'male' AND last_name >= 'Doe') OR first_name = 'Tom'
		records := executePlan(t, statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			statement.NewWhereClause(
				expression.NewBinaryExpr(
					"OR",
					expression.NewLhsExpr(
						expression.NewBinaryExpr(
							"AND",
							expression.NewLhsExpr(
								expression.NewBinaryExpr(
									"<",
									expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
									expression.NewRhsLiteral(literal.NewStringLiteral("K", "K")),
								),
							),
							expression.NewRhsExpr(
								expression.NewBinaryExpr(
									"AND",
									expression.NewLhsExpr(
										expression.NewBinaryExpr(
											"=",
											expression.NewLhsColumn(*identifier.NewColumnId("gender")),
											expression.NewRhsLiteral(literal.NewStringLiteral("male", "male")),
										),
									),
									expression.NewRhsExpr(
										expression.NewBinaryExpr(
											">=",
											expression.NewLhsColumn(*identifier.NewColumnId("last_name")),
											expression.NewRhsLiteral(literal.NewStringLiteral("Doe", "Doe")),
										),
									),
								),
							),
						),
					),
					expression.NewRhsExpr(
						expression.NewBinaryExpr(
							"=",
							expression.NewLhsColumn(*identifier.NewColumnId("first_name")),
							expression.NewRhsLiteral(literal.NewStringLiteral("Tom", "Tom")),
						),
					),
				),
			),
		))

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

	t.Run("UPDATE でレコードを更新できる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		defer engine.Reset()

		// WHEN (UPDATE users SET last_name = 'Smith' WHERE username = 'johndoe')
		executePlan(t, &statement.UpdateStmt{
			Table: *identifier.NewTableId("users"),
			SetClauses: []*statement.SetClause{
				{Column: *identifier.NewColumnId("last_name"), Value: literal.NewStringLiteral("'Smith'", "Smith")},
			},
			Where: statement.NewWhereClause(
				expression.NewBinaryExpr(
					"=",
					expression.NewLhsColumn(*identifier.NewColumnId("username")),
					expression.NewRhsLiteral(literal.NewStringLiteral("johndoe", "johndoe")),
				),
			),
		})

		// THEN
		records := executePlan(t, statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			nil,
		))

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

		// WHEN
		executePlan(t, statement.NewDeleteStmt(
			*identifier.NewTableId("users"),
			statement.NewWhereClause(
				expression.NewBinaryExpr(
					"=",
					expression.NewLhsColumn(*identifier.NewColumnId("username")),
					expression.NewRhsLiteral(literal.NewStringLiteral("johndoe2", "johndoe2")),
				),
			),
		))

		// THEN
		records := executePlan(t, statement.NewSelectStmt(
			*identifier.NewTableId("users"),
			nil,
		))

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

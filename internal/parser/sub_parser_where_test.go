package parser

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/ast"
	"github.com/stretchr/testify/assert"
)

// WHERE 句のパースロジックは WhereParser で共通化されているため、
// SELECT 文を経由して WHERE 句のパースを網羅的にテストする。

func TestParserWhere(t *testing.T) {
	t.Run("AND 演算子を使った WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1' AND name = 'John';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		assert.NotNil(t, selectStmt.Where)

		binaryExpr := selectStmt.Where.Condition
		assert.Equal(t, "AND", binaryExpr.Operator)

		// 左辺: id = '1'
		leftExpr, ok := binaryExpr.Left.(*ast.LhsExpr)
		assert.True(t, ok)
		leftBinary := leftExpr.Expr
		assert.Equal(t, "=", leftBinary.Operator)

		leftCol, ok := leftBinary.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", leftCol.Column.ColName)

		leftLit, ok := leftBinary.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", leftLit.Literal.ToString())

		// 右辺: name = 'John'
		rightExpr, ok := binaryExpr.Right.(*ast.RhsExpr)
		assert.True(t, ok)
		rightBinary := rightExpr.Expr
		assert.Equal(t, "=", rightBinary.Operator)

		rightCol, ok := rightBinary.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "name", rightCol.Column.ColName)

		rightLit, ok := rightBinary.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "John", rightLit.Literal.ToString())
	})

	t.Run("OR 演算子を使った WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = '1' OR id = '2';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		binaryExpr := selectStmt.Where.Condition
		assert.Equal(t, "OR", binaryExpr.Operator)

		// 左辺: id = '1'
		leftExpr, ok := binaryExpr.Left.(*ast.LhsExpr)
		assert.True(t, ok)
		leftBinary := leftExpr.Expr
		assert.Equal(t, "=", leftBinary.Operator)

		leftCol, ok := leftBinary.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", leftCol.Column.ColName)

		leftLit, ok := leftBinary.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", leftLit.Literal.ToString())

		// 右辺: id = '2'
		rightExpr, ok := binaryExpr.Right.(*ast.RhsExpr)
		assert.True(t, ok)
		rightBinary := rightExpr.Expr
		assert.Equal(t, "=", rightBinary.Operator)

		rightCol, ok := rightBinary.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "id", rightCol.Column.ColName)

		rightLit, ok := rightBinary.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "2", rightLit.Literal.ToString())
	})

	t.Run("AND と OR の優先順位が正しく処理される", func(t *testing.T) {
		// GIVEN: AND が OR より先に結合される
		// a = '1' OR b = '2' AND c = '3' → (a = '1') OR ((b = '2') AND (c = '3'))
		sql := "SELECT * FROM users WHERE a = '1' OR b = '2' AND c = '3';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		// ルートは OR
		orExpr := selectStmt.Where.Condition
		assert.Equal(t, "OR", orExpr.Operator)

		// OR の左辺: a = '1'
		orLeftExpr, ok := orExpr.Left.(*ast.LhsExpr)
		assert.True(t, ok)
		aEqExpr := orLeftExpr.Expr
		assert.Equal(t, "=", aEqExpr.Operator)
		aCol, ok := aEqExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "a", aCol.Column.ColName)
		aLit, ok := aEqExpr.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", aLit.Literal.ToString())

		// OR の右辺: (b = '2') AND (c = '3')
		orRightExpr, ok := orExpr.Right.(*ast.RhsExpr)
		assert.True(t, ok)
		andExpr := orRightExpr.Expr
		assert.Equal(t, "AND", andExpr.Operator)

		// AND の左辺: b = '2'
		andLeftExpr, ok := andExpr.Left.(*ast.LhsExpr)
		assert.True(t, ok)
		bEqExpr := andLeftExpr.Expr
		assert.Equal(t, "=", bEqExpr.Operator)
		bCol, ok := bEqExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "b", bCol.Column.ColName)

		// AND の右辺: c = '3'
		andRightExpr, ok := andExpr.Right.(*ast.RhsExpr)
		assert.True(t, ok)
		cEqExpr := andRightExpr.Expr
		assert.Equal(t, "=", cEqExpr.Operator)
		cCol, ok := cEqExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "c", cCol.Column.ColName)
	})

	t.Run("3 つ以上の AND 条件をパースできる", func(t *testing.T) {
		// GIVEN: 左結合で ((a = '1') AND (b = '2')) AND (c = '3') になる
		sql := "SELECT * FROM users WHERE a = '1' AND b = '2' AND c = '3';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		// ルートは AND (右側の AND)
		rootExpr := selectStmt.Where.Condition
		assert.Equal(t, "AND", rootExpr.Operator)

		// 左辺: (a = '1') AND (b = '2')
		leftExpr, ok := rootExpr.Left.(*ast.LhsExpr)
		assert.True(t, ok)
		innerAndExpr := leftExpr.Expr
		assert.Equal(t, "AND", innerAndExpr.Operator)

		// 内側 AND の左辺: a = '1'
		innerLeftExpr, ok := innerAndExpr.Left.(*ast.LhsExpr)
		assert.True(t, ok)
		aEqExpr := innerLeftExpr.Expr
		aCol, ok := aEqExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "a", aCol.Column.ColName)

		// 内側 AND の右辺: b = '2'
		innerRightExpr, ok := innerAndExpr.Right.(*ast.RhsExpr)
		assert.True(t, ok)
		bEqExpr := innerRightExpr.Expr
		bCol, ok := bEqExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "b", bCol.Column.ColName)

		// ルートの右辺: c = '3'
		rightExpr, ok := rootExpr.Right.(*ast.RhsExpr)
		assert.True(t, ok)
		cEqExpr := rightExpr.Expr
		cCol, ok := cEqExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "c", cCol.Column.ColName)
	})

	t.Run("比較演算子を使った WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id > '10';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		binaryExpr := selectStmt.Where.Condition
		assert.Equal(t, ">", binaryExpr.Operator)
	})

	t.Run("数値リテラルを含む WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id = 42;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		binaryExpr := selectStmt.Where.Condition
		assert.Equal(t, "=", binaryExpr.Operator)

		rhsLit, ok := binaryExpr.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "42", rhsLit.Literal.ToString())
	})

	t.Run("<> 演算子を AND と組み合わせてパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE a <> '1' AND b = '2';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		// ルートは AND
		andExpr := selectStmt.Where.Condition
		assert.Equal(t, "AND", andExpr.Operator)

		// AND の左辺: a <> '1'
		leftExpr, ok := andExpr.Left.(*ast.LhsExpr)
		assert.True(t, ok)
		neqExpr := leftExpr.Expr
		assert.Equal(t, "<>", neqExpr.Operator)

		leftCol, ok := neqExpr.Left.(*ast.LhsColumn)
		assert.True(t, ok)
		assert.Equal(t, "a", leftCol.Column.ColName)

		leftLit, ok := neqExpr.Right.(*ast.RhsLiteral)
		assert.True(t, ok)
		assert.Equal(t, "1", leftLit.Literal.ToString())
	})

	t.Run("2 文字比較演算子を使った WHERE 句をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "SELECT * FROM users WHERE id >= '10';"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		selectStmt, ok := result.(*ast.SelectStmt)
		assert.True(t, ok)

		binaryExpr := selectStmt.Where.Condition
		assert.Equal(t, ">=", binaryExpr.Operator)
	})

	t.Run("不正な WHERE 句でエラーになる", func(t *testing.T) {
		t.Run("WHERE 句の式が不完全な場合 (演算子のみ)", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users WHERE id =;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expression")
		})

		t.Run("WHERE 句の式が不完全な場合 (左辺のみ)", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users WHERE id;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expression")
		})

		t.Run("WHERE 句が空の場合", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users WHERE;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "empty expression")
		})

		t.Run("AND/OR の後に式がない場合", func(t *testing.T) {
			// GIVEN
			sql := "SELECT * FROM users WHERE id = '1' AND;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expression")
		})
	})
}

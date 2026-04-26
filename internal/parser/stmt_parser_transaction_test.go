package parser

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/ast"
	"github.com/stretchr/testify/assert"
)

func TestParserTransaction(t *testing.T) {
	t.Run("BEGIN をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "BEGIN;"
		p := NewParser()

		// WHEN
		result, err := p.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxBegin, stmt.Kind)
	})

	t.Run("COMMIT をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "COMMIT;"
		p := NewParser()

		// WHEN
		result, err := p.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxCommit, stmt.Kind)
	})

	t.Run("ROLLBACK をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "ROLLBACK;"
		p := NewParser()

		// WHEN
		result, err := p.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxRollback, stmt.Kind)
	})

	t.Run("小文字の begin をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "begin;"
		p := NewParser()

		// WHEN
		result, err := p.Parse(sql)

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxBegin, stmt.Kind)
	})
}

func TestParserStartTransaction(t *testing.T) {
	t.Run("START TRANSACTION をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "START TRANSACTION;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxBegin, stmt.Kind)
	})

	t.Run("セミコロンなしでもパースできる", func(t *testing.T) {
		// GIVEN
		sql := "START TRANSACTION"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxBegin, stmt.Kind)
	})

	t.Run("小文字でもパースできる", func(t *testing.T) {
		// GIVEN
		sql := "start transaction;"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		stmt, ok := result.(*ast.TransactionStmt)
		assert.True(t, ok)
		assert.Equal(t, ast.TxBegin, stmt.Kind)
	})

	t.Run("不正な START 文でエラーになる", func(t *testing.T) {
		t.Run("START の後に TRANSACTION 以外が来た場合", func(t *testing.T) {
			// GIVEN
			sql := "START SELECT;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expected TRANSACTION after START")
		})

		t.Run("START のみの場合", func(t *testing.T) {
			// GIVEN
			sql := "START;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})
	})
}

func TestParserEmptyInput(t *testing.T) {
	t.Run("空文字列をパースするとエラーが返る", func(t *testing.T) {
		// GIVEN
		sql := ""
		p := NewParser()

		// WHEN
		result, err := p.Parse(sql)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("セミコロンのみをパースするとエラーが返る", func(t *testing.T) {
		// GIVEN
		sql := ";"
		p := NewParser()

		// WHEN
		result, err := p.Parse(sql)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

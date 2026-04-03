package parser

import (
	"minesql/internal/ast"
	"testing"

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

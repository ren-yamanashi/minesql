package parser

import (
	"minesql/internal/ast"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnDefParser(t *testing.T) {
	t.Run("VARCHAR カラムを正しくパースできる", func(t *testing.T) {
		// GIVEN
		cp := NewColumnDefParser("id")

		// WHEN
		cp.onKeyword("VARCHAR")
		err := cp.finalize()

		// THEN
		assert.NoError(t, err)
		def := cp.getDef()
		colDef, ok := def.(*ast.ColumnDef)
		assert.True(t, ok)
		assert.Equal(t, "id", colDef.ColName)
		assert.Equal(t, ast.DataTypeVarchar, colDef.DataType)
	})

	t.Run("小文字の varchar もパースできる", func(t *testing.T) {
		// GIVEN
		cp := NewColumnDefParser("name")

		// WHEN
		cp.onKeyword("varchar")
		err := cp.finalize()

		// THEN
		assert.NoError(t, err)
		colDef := cp.getDef().(*ast.ColumnDef)
		assert.Equal(t, "name", colDef.ColName)
		assert.Equal(t, ast.DataTypeVarchar, colDef.DataType)
	})

	t.Run("データ型が指定されない場合、finalize でエラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewColumnDefParser("id")

		// WHEN
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "data type is required")
		assert.Contains(t, err.Error(), "id")
	})

	t.Run("サポートされていないデータ型の場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewColumnDefParser("age")

		// WHEN
		cp.onKeyword("INT")
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only VARCHAR is supported")
	})

	t.Run("データ型の後にさらにキーワードが来た場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewColumnDefParser("id")

		// WHEN
		cp.onKeyword("VARCHAR")
		cp.onKeyword("NOT")
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected keyword after data type")
	})

	t.Run("エラー発生後のキーワードは無視される", func(t *testing.T) {
		// GIVEN
		cp := NewColumnDefParser("id")

		// WHEN
		cp.onKeyword("INT")     // エラー発生
		cp.onKeyword("VARCHAR") // 無視される

		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only VARCHAR is supported")
	})
}

package parser

import (
	"minesql/internal/ast"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstraintDefParser_PrimaryKey(t *testing.T) {
	t.Run("単一カラムの PRIMARY KEY をパースできる", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("PRIMARY")
		cp.onKeyword("KEY")
		cp.onSymbol("(")
		cp.onIdentifier("id")
		cp.onSymbol(")")
		err := cp.finalize()

		// THEN
		assert.NoError(t, err)
		def := cp.getDef()
		pkDef, ok := def.(*ast.ConstraintPrimaryKeyDef)
		assert.True(t, ok)
		assert.Equal(t, 1, len(pkDef.Columns))
		assert.Equal(t, "id", pkDef.Columns[0].ColName)
	})

	t.Run("複数カラムの PRIMARY KEY をパースできる", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("PRIMARY")
		cp.onKeyword("KEY")
		cp.onSymbol("(")
		cp.onIdentifier("id")
		cp.onSymbol(",")
		cp.onIdentifier("tenant_id")
		cp.onSymbol(")")
		err := cp.finalize()

		// THEN
		assert.NoError(t, err)
		pkDef := cp.getDef().(*ast.ConstraintPrimaryKeyDef)
		assert.Equal(t, 2, len(pkDef.Columns))
		assert.Equal(t, "id", pkDef.Columns[0].ColName)
		assert.Equal(t, "tenant_id", pkDef.Columns[1].ColName)
	})

	t.Run("PRIMARY KEY にカラムが指定されていない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN: "(" の直後に ")" が来ると、カラム待ちの状態で予期しないシンボル
		cp.onKeyword("PRIMARY")
		cp.onKeyword("KEY")
		cp.onSymbol("(")
		cp.onSymbol(")")
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected symbol in constraint")
	})

	t.Run("PRIMARY KEY に名前を付けようとした場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("PRIMARY")
		cp.onKeyword("KEY")
		cp.onIdentifier("pk_name")
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "PRIMARY KEY name not supported")
	})
}

func TestConstraintDefParser_UniqueKey(t *testing.T) {
	t.Run("UNIQUE KEY をパースできる", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("UNIQUE")
		cp.onKeyword("KEY")
		cp.onIdentifier("idx_email")
		cp.onSymbol("(")
		cp.onIdentifier("email")
		cp.onSymbol(")")
		err := cp.finalize()

		// THEN
		assert.NoError(t, err)
		def := cp.getDef()
		ukDef, ok := def.(*ast.ConstraintUniqueKeyDef)
		assert.True(t, ok)
		assert.Equal(t, "idx_email", ukDef.KeyName)
		assert.Equal(t, "email", ukDef.Column.ColName)
	})

	t.Run("UNIQUE KEY のカラムが指定されていない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN: "(" の直後に ")" が来ると、カラム待ちの状態で予期しないシンボル
		cp.onKeyword("UNIQUE")
		cp.onKeyword("KEY")
		cp.onIdentifier("idx_email")
		cp.onSymbol("(")
		cp.onSymbol(")")
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected symbol in constraint")
	})

	t.Run("UNIQUE KEY のキー名が重複して指定された場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("UNIQUE")
		cp.onKeyword("KEY")
		cp.onIdentifier("idx_1")
		cp.onIdentifier("idx_2") // 2 つ目のキー名
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "key name already set")
	})
}

func TestConstraintDefParser_Error(t *testing.T) {
	t.Run("PRIMARY でも UNIQUE でもないキーワードの場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("FOREIGN")
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected 'PRIMARY' or 'UNIQUE'")
	})

	t.Run("KEY キーワードがない場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("PRIMARY")
		cp.onKeyword("INDEX") // KEY ではない
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected 'KEY'")
	})

	t.Run("KEY の後に予期しないシンボルが来た場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("PRIMARY")
		cp.onKeyword("KEY")
		cp.onSymbol(",") // "(" ではない
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected symbol in constraint")
	})

	t.Run("カラムリストの区切りに予期しないシンボルが来た場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("PRIMARY")
		cp.onKeyword("KEY")
		cp.onSymbol("(")
		cp.onIdentifier("id")
		cp.onSymbol("=") // "," でも ")" でもない
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected symbol in constraint")
	})

	t.Run("エラー発生後のトークンは無視される", func(t *testing.T) {
		// GIVEN
		cp := NewConstraintDefParser()

		// WHEN
		cp.onKeyword("FOREIGN") // エラー発生
		cp.onKeyword("KEY")     // 無視される
		cp.onSymbol("(")        // 無視される
		cp.onIdentifier("id")   // 無視される
		err := cp.finalize()

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected 'PRIMARY' or 'UNIQUE'")
	})
}

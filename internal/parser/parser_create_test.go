package parser

import (
	"minesql/internal/planner/ast/definition"
	"minesql/internal/planner/ast/statement"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserCreateTable(t *testing.T) {
	t.Run("基本的な CREATE TABLE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "CREATE TABLE users (id VARCHAR, name VARCHAR);"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		createStmt, ok := result.(*statement.CreateTableStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", createStmt.TableName)
		assert.Equal(t, statement.KeywordTable, createStmt.Keyword)
		assert.Equal(t, 2, len(createStmt.CreateDefinitions))

		// カラム定義の検証
		colDef1, ok := createStmt.CreateDefinitions[0].(*definition.ColumnDef)
		assert.True(t, ok)
		assert.Equal(t, "id", colDef1.ColName)
		assert.Equal(t, definition.DataTypeVarchar, colDef1.DataType)

		colDef2, ok := createStmt.CreateDefinitions[1].(*definition.ColumnDef)
		assert.True(t, ok)
		assert.Equal(t, "name", colDef2.ColName)
		assert.Equal(t, definition.DataTypeVarchar, colDef2.DataType)
	})

	t.Run("PRIMARY KEY 制約を含む CREATE TABLE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "CREATE TABLE users (id VARCHAR, name VARCHAR, PRIMARY KEY (id));"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		createStmt, ok := result.(*statement.CreateTableStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", createStmt.TableName)
		assert.Equal(t, 3, len(createStmt.CreateDefinitions))

		// PRIMARY KEY 制約の検証
		pkDef, ok := createStmt.CreateDefinitions[2].(*definition.ConstraintPrimaryKeyDef)
		assert.True(t, ok)
		assert.Equal(t, definition.DefTypeConstraintPrimaryKey, pkDef.DefType)
		assert.Equal(t, 1, len(pkDef.Columns))
		assert.Equal(t, "id", pkDef.Columns[0].ColName)
	})

	t.Run("複数カラムの PRIMARY KEY をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "CREATE TABLE users (id VARCHAR, email VARCHAR, PRIMARY KEY (id, email));"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		createStmt, ok := result.(*statement.CreateTableStmt)
		assert.True(t, ok)

		pkDef, ok := createStmt.CreateDefinitions[2].(*definition.ConstraintPrimaryKeyDef)
		assert.True(t, ok)
		assert.Equal(t, 2, len(pkDef.Columns))
		assert.Equal(t, "id", pkDef.Columns[0].ColName)
		assert.Equal(t, "email", pkDef.Columns[1].ColName)
	})

	t.Run("UNIQUE KEY 制約を含む CREATE TABLE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "CREATE TABLE users (id VARCHAR, email VARCHAR, UNIQUE KEY email_idx (email));"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		createStmt, ok := result.(*statement.CreateTableStmt)
		assert.True(t, ok)

		assert.Equal(t, 3, len(createStmt.CreateDefinitions))

		// UNIQUE KEY 制約の検証
		ukDef, ok := createStmt.CreateDefinitions[2].(*definition.ConstraintUniqueKeyDef)
		assert.True(t, ok)
		assert.Equal(t, definition.DefTypeConstraintUniqueKey, ukDef.DefType)
		assert.Equal(t, "email_idx", ukDef.KeyName)
		assert.Equal(t, "email", ukDef.Column.ColName)
	})

	t.Run("複数の制約を含む CREATE TABLE 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "CREATE TABLE users (id VARCHAR, name VARCHAR, email VARCHAR, PRIMARY KEY (id), UNIQUE KEY email_idx (email));"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		createStmt, ok := result.(*statement.CreateTableStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", createStmt.TableName)
		assert.Equal(t, 5, len(createStmt.CreateDefinitions))

		// カラム定義
		colDef1, ok := createStmt.CreateDefinitions[0].(*definition.ColumnDef)
		assert.True(t, ok)
		assert.Equal(t, "id", colDef1.ColName)

		colDef2, ok := createStmt.CreateDefinitions[1].(*definition.ColumnDef)
		assert.True(t, ok)
		assert.Equal(t, "name", colDef2.ColName)

		colDef3, ok := createStmt.CreateDefinitions[2].(*definition.ColumnDef)
		assert.True(t, ok)
		assert.Equal(t, "email", colDef3.ColName)

		// PRIMARY KEY
		pkDef, ok := createStmt.CreateDefinitions[3].(*definition.ConstraintPrimaryKeyDef)
		assert.True(t, ok)
		assert.Equal(t, 1, len(pkDef.Columns))
		assert.Equal(t, "id", pkDef.Columns[0].ColName)

		// UNIQUE KEY
		ukDef, ok := createStmt.CreateDefinitions[4].(*definition.ConstraintUniqueKeyDef)
		assert.True(t, ok)
		assert.Equal(t, "email_idx", ukDef.KeyName)
		assert.Equal(t, "email", ukDef.Column.ColName)
	})

	t.Run("コメント付きの CREATE TABLE 文をパースできる", func(t *testing.T) {
		t.Run("行コメント付き", func(t *testing.T) {
			// GIVEN
			sql := `
-- これはコメント
CREATE TABLE users ( -- テーブル定義開始
    id VARCHAR, -- ID カラム
    name VARCHAR, -- 名前カラム
    PRIMARY KEY (id) -- 主キー制約
) -- テーブル定義終了
;
`
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.NoError(t, err)
			assert.NotNil(t, result)

			createStmt, ok := result.(*statement.CreateTableStmt)
			assert.True(t, ok)
			assert.Equal(t, "users", createStmt.TableName)
			assert.Equal(t, 3, len(createStmt.CreateDefinitions))

			// カラム定義の検証
			colDef1, ok := createStmt.CreateDefinitions[0].(*definition.ColumnDef)
			assert.True(t, ok)
			assert.Equal(t, "id", colDef1.ColName)

			colDef2, ok := createStmt.CreateDefinitions[1].(*definition.ColumnDef)
			assert.True(t, ok)
			assert.Equal(t, "name", colDef2.ColName)

			// PRIMARY KEY 制約の検証
			pkDef, ok := createStmt.CreateDefinitions[2].(*definition.ConstraintPrimaryKeyDef)
			assert.True(t, ok)
			assert.Equal(t, "id", pkDef.Columns[0].ColName)
		})

		t.Run("ブロックコメント付き", func(t *testing.T) {
			// GIVEN
			sql := `
/* これはコメント */
CREATE TABLE users ( /* テーブル定義開始 */
    id VARCHAR, /* ID カラム */
    name VARCHAR, /* 名前カラム */
    PRIMARY KEY (id) /* 主キー制約 */
) /* テーブル定義終了 */
;
`
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.NoError(t, err)
			assert.NotNil(t, result)

			createStmt, ok := result.(*statement.CreateTableStmt)
			assert.True(t, ok)
			assert.Equal(t, "users", createStmt.TableName)
			assert.Equal(t, 3, len(createStmt.CreateDefinitions))

			// カラム定義の検証
			colDef1, ok := createStmt.CreateDefinitions[0].(*definition.ColumnDef)
			assert.True(t, ok)
			assert.Equal(t, "id", colDef1.ColName)

			colDef2, ok := createStmt.CreateDefinitions[1].(*definition.ColumnDef)
			assert.True(t, ok)
			assert.Equal(t, "name", colDef2.ColName)

			// PRIMARY KEY 制約の検証
			pkDef, ok := createStmt.CreateDefinitions[2].(*definition.ConstraintPrimaryKeyDef)
			assert.True(t, ok)
			assert.Equal(t, "id", pkDef.Columns[0].ColName)
		})
	})

	t.Run("不正な CREATE TABLE 文でエラーになる", func(t *testing.T) {
		t.Run("TABLE キーワードがない場合", func(t *testing.T) {
			// GIVEN
			sql := "CREATE users (id VARCHAR);"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("テーブル名がない場合", func(t *testing.T) {
			// GIVEN
			sql := "CREATE TABLE (id VARCHAR);"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("カラム定義がない場合", func(t *testing.T) {
			// GIVEN
			sql := "CREATE TABLE users ();"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("データ型がない場合", func(t *testing.T) {
			// GIVEN
			sql := "CREATE TABLE users (id, name VARCHAR);"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("PRIMARY KEY のカラムリストが空の場合", func(t *testing.T) {
			// GIVEN
			sql := "CREATE TABLE users (id VARCHAR, PRIMARY KEY ());"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("UNIQUE KEY のカラム指定がない場合", func(t *testing.T) {
			// GIVEN
			sql := "CREATE TABLE users (id VARCHAR, UNIQUE KEY idx_name ());"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("末尾にセミコロンがない場合", func(t *testing.T) {
			// GIVEN
			sql := "CREATE TABLE users (id VARCHAR, name VARCHAR)"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "incomplete CREATE TABLE statement")
		})
	})
}

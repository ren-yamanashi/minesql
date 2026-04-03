package parser

import (
	"minesql/internal/ast"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserInsert(t *testing.T) {
	t.Run("基本的な INSERT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "INSERT INTO users (id, name) VALUES ('1', 'John');"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		insertStmt, ok := result.(*ast.InsertStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", insertStmt.Table.TableName)

		// カラム数の確認
		assert.Equal(t, 2, len(insertStmt.Cols))
		assert.Equal(t, "id", insertStmt.Cols[0].ColName)
		assert.Equal(t, "name", insertStmt.Cols[1].ColName)

		// 値の確認
		assert.Equal(t, 1, len(insertStmt.Values))
		assert.Equal(t, 2, len(insertStmt.Values[0]))
		assert.Equal(t, "1", insertStmt.Values[0][0].ToString())
		assert.Equal(t, "John", insertStmt.Values[0][1].ToString())
	})

	t.Run("複数行の INSERT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "INSERT INTO users (id, name) VALUES ('1', 'John'), ('2', 'Jane');"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		insertStmt, ok := result.(*ast.InsertStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", insertStmt.Table.TableName)
		assert.Equal(t, 2, len(insertStmt.Cols))

		// 2行の値を確認
		assert.Equal(t, 2, len(insertStmt.Values))

		// 1行目
		assert.Equal(t, 2, len(insertStmt.Values[0]))
		assert.Equal(t, "1", insertStmt.Values[0][0].ToString())
		assert.Equal(t, "John", insertStmt.Values[0][1].ToString())

		// 2行目
		assert.Equal(t, 2, len(insertStmt.Values[1]))
		assert.Equal(t, "2", insertStmt.Values[1][0].ToString())
		assert.Equal(t, "Jane", insertStmt.Values[1][1].ToString())
	})

	t.Run("3行以上の INSERT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "INSERT INTO users (id, name, email) VALUES ('1', 'John', 'john@example.com'), ('2', 'Jane', 'jane@example.com'), ('3', 'Bob', 'bob@example.com');"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		insertStmt, ok := result.(*ast.InsertStmt)
		assert.True(t, ok)

		assert.Equal(t, "users", insertStmt.Table.TableName)
		assert.Equal(t, 3, len(insertStmt.Cols))
		assert.Equal(t, 3, len(insertStmt.Values))

		// 各行の値を確認
		assert.Equal(t, "1", insertStmt.Values[0][0].ToString())
		assert.Equal(t, "John", insertStmt.Values[0][1].ToString())
		assert.Equal(t, "john@example.com", insertStmt.Values[0][2].ToString())

		assert.Equal(t, "2", insertStmt.Values[1][0].ToString())
		assert.Equal(t, "Jane", insertStmt.Values[1][1].ToString())
		assert.Equal(t, "jane@example.com", insertStmt.Values[1][2].ToString())

		assert.Equal(t, "3", insertStmt.Values[2][0].ToString())
		assert.Equal(t, "Bob", insertStmt.Values[2][1].ToString())
		assert.Equal(t, "bob@example.com", insertStmt.Values[2][2].ToString())
	})

	t.Run("数値を含む INSERT 文をパースできる", func(t *testing.T) {
		// GIVEN
		sql := "INSERT INTO users (id, age) VALUES ('1', 25);"
		parser := NewParser()

		// WHEN
		result, err := parser.Parse(sql)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, result)

		insertStmt, ok := result.(*ast.InsertStmt)
		assert.True(t, ok)

		assert.Equal(t, 1, len(insertStmt.Values))
		assert.Equal(t, "1", insertStmt.Values[0][0].ToString())
		assert.Equal(t, "25", insertStmt.Values[0][1].ToString())
	})

	t.Run("コメント付きの INSERT 文をパースできる", func(t *testing.T) {
		t.Run("行コメント付き", func(t *testing.T) {
			// GIVEN
			sql := `
-- これはコメント
INSERT INTO users (id, name) -- カラムリスト
VALUES ('1', 'John'); -- 値リスト
`
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.NoError(t, err)
			assert.NotNil(t, result)

			insertStmt, ok := result.(*ast.InsertStmt)
			assert.True(t, ok)
			assert.Equal(t, "users", insertStmt.Table.TableName)
			assert.Equal(t, 2, len(insertStmt.Cols))
			assert.Equal(t, "id", insertStmt.Cols[0].ColName)
			assert.Equal(t, "name", insertStmt.Cols[1].ColName)
			assert.Equal(t, 1, len(insertStmt.Values))
			assert.Equal(t, "1", insertStmt.Values[0][0].ToString())
			assert.Equal(t, "John", insertStmt.Values[0][1].ToString())
		})

		t.Run("ブロックコメント付き", func(t *testing.T) {
			// GIVEN
			sql := `
/* これはコメント */
INSERT INTO users (id, name) /* カラムリスト */
VALUES ('1', 'John') /* 値リスト */
;
`
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.NoError(t, err)
			assert.NotNil(t, result)

			insertStmt, ok := result.(*ast.InsertStmt)
			assert.True(t, ok)
			assert.Equal(t, "users", insertStmt.Table.TableName)
			assert.Equal(t, 2, len(insertStmt.Cols))
			assert.Equal(t, "id", insertStmt.Cols[0].ColName)
			assert.Equal(t, "name", insertStmt.Cols[1].ColName)
			assert.Equal(t, 1, len(insertStmt.Values))
			assert.Equal(t, "1", insertStmt.Values[0][0].ToString())
			assert.Equal(t, "John", insertStmt.Values[0][1].ToString())
		})
	})

	t.Run("不正な INSERT 文でエラーになる", func(t *testing.T) {
		t.Run("カラムリストなしの場合", func(t *testing.T) {
			// GIVEN
			sql := "INSERT INTO users VALUES ('1', 'John');"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "column list is required")
		})

		t.Run("カラムリストが空の場合", func(t *testing.T) {
			// GIVEN
			sql := "INSERT INTO users () VALUES ('1', 'John');"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "column list is required")
		})

		t.Run("VALUES がない場合", func(t *testing.T) {
			// GIVEN
			sql := "INSERT INTO users (id, name);"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("値リストが空の場合", func(t *testing.T) {
			// GIVEN
			sql := "INSERT INTO users (id, name) VALUES;"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
		})

		t.Run("末尾にセミコロンがない場合", func(t *testing.T) {
			// GIVEN
			sql := "INSERT INTO users (id, name) VALUES ('1', 'John')"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "incomplete INSERT statement")
		})

		t.Run("INTO キーワードがない場合", func(t *testing.T) {
			// GIVEN: INTO の代わりに SELECT が来る
			sql := "INSERT SELECT users (id) VALUES ('1');"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expected INTO")
		})

		t.Run("テーブル名の後にカラムリスト開始の ( がない場合", func(t *testing.T) {
			// GIVEN
			sql := "INSERT INTO users = 'Jane';"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "expected '('")
		})

		t.Run("不正な位置で文字列リテラルが来た場合", func(t *testing.T) {
			// GIVEN: テーブル名の位置に文字列リテラル
			sql := "INSERT INTO 'users' (id) VALUES ('1');"
			parser := NewParser()

			// WHEN
			result, err := parser.Parse(sql)

			// THEN
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), "unexpected string")
		})
	})
}

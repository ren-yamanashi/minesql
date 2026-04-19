package planner

import (
	"minesql/internal/ast"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/handler"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveJoinedColumns(t *testing.T) {
	t.Run("単一テーブルのカラム位置が正しい", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		hdl := handler.Get()
		usersMeta, _ := hdl.Catalog.GetTableMetaByName("users")

		// WHEN
		columns := resolveJoinedColumns([]*handler.TableMetadata{usersMeta})

		// THEN: users は id, first_name, last_name, gender, username の 5 カラム
		assert.Len(t, columns, 5)
		assert.Equal(t, "id", columns[0].colName)
		assert.Equal(t, 0, columns[0].pos)
		assert.Equal(t, "users", columns[0].tableName)
		assert.Equal(t, 4, columns[4].pos)
	})

	t.Run("2 テーブルで 2 テーブル目の位置が 1 テーブル目のカラム数から始まる", func(t *testing.T) {
		// GIVEN: 2 テーブル分のメタデータを手動構築
		tbl1 := &handler.TableMetadata{
			Name:  "users",
			NCols: 3,
			Cols: []*dictionary.ColumnMeta{
				{Name: "id", Pos: 0},
				{Name: "name", Pos: 1},
				{Name: "age", Pos: 2},
			},
		}
		tbl2 := &handler.TableMetadata{
			Name:  "orders",
			NCols: 2,
			Cols: []*dictionary.ColumnMeta{
				{Name: "id", Pos: 0},
				{Name: "user_id", Pos: 1},
			},
		}

		// WHEN
		columns := resolveJoinedColumns([]*handler.TableMetadata{tbl1, tbl2})

		// THEN: 合計 5 カラム、orders の先頭は pos=3
		assert.Len(t, columns, 5)
		// users
		assert.Equal(t, "users", columns[0].tableName)
		assert.Equal(t, "id", columns[0].colName)
		assert.Equal(t, 0, columns[0].pos)
		assert.Equal(t, 2, columns[2].pos)
		// orders: 1 テーブル目の 3 カラム分のオフセット
		assert.Equal(t, "orders", columns[3].tableName)
		assert.Equal(t, "id", columns[3].colName)
		assert.Equal(t, 3, columns[3].pos)
		assert.Equal(t, "user_id", columns[4].colName)
		assert.Equal(t, 4, columns[4].pos)
	})
}

func TestFindColumnPos(t *testing.T) {
	// GIVEN: users(id, name) + orders(id, user_id) の結合カラム
	columns := []joinedColumn{
		{tableName: "users", colName: "id", pos: 0},
		{tableName: "users", colName: "name", pos: 1},
		{tableName: "orders", colName: "id", pos: 2},
		{tableName: "orders", colName: "user_id", pos: 3},
	}

	t.Run("修飾名で一意に特定できる", func(t *testing.T) {
		// WHEN
		pos, err := findColumnPos(columns, "orders", "user_id")

		// THEN
		require.NoError(t, err)
		assert.Equal(t, 3, pos)
	})

	t.Run("非修飾名で一意のカラムを特定できる", func(t *testing.T) {
		// WHEN: "name" は users にのみ存在
		pos, err := findColumnPos(columns, "", "name")

		// THEN
		require.NoError(t, err)
		assert.Equal(t, 1, pos)
	})

	t.Run("非修飾名で曖昧な場合エラーになる", func(t *testing.T) {
		// WHEN: "id" は users と orders の両方に存在
		_, err := findColumnPos(columns, "", "id")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ambiguous")
	})

	t.Run("存在しないカラムでエラーになる", func(t *testing.T) {
		// WHEN
		_, err := findColumnPos(columns, "users", "nonexistent")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("存在しないテーブルでエラーになる", func(t *testing.T) {
		// WHEN
		_, err := findColumnPos(columns, "nonexistent", "id")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestResolveSelectColumns(t *testing.T) {
	t.Run("SELECT * で全カラムの位置が返される", func(t *testing.T) {
		// GIVEN
		tbl := &handler.TableMetadata{
			Name: "users", NCols: 3,
			Cols: []*dictionary.ColumnMeta{
				{Name: "id", Pos: 0}, {Name: "name", Pos: 1}, {Name: "age", Pos: 2},
			},
		}

		// WHEN
		pos, err := resolveSelectColumns(nil, []*handler.TableMetadata{tbl})

		// THEN
		require.NoError(t, err)
		assert.Equal(t, []uint16{0, 1, 2}, pos)
	})

	t.Run("指定カラムの位置が解決される", func(t *testing.T) {
		// GIVEN
		tbl := &handler.TableMetadata{
			Name: "users", NCols: 3,
			Cols: []*dictionary.ColumnMeta{
				{Name: "id", Pos: 0}, {Name: "name", Pos: 1}, {Name: "age", Pos: 2},
			},
		}

		// WHEN: age, id の順で指定
		pos, err := resolveSelectColumns(
			[]ast.ColumnId{{ColName: "age"}, {ColName: "id"}},
			[]*handler.TableMetadata{tbl},
		)

		// THEN: カラム位置が指定順に返される
		require.NoError(t, err)
		assert.Equal(t, []uint16{2, 0}, pos)
	})

	t.Run("存在しないカラムでエラーになる", func(t *testing.T) {
		// GIVEN
		tbl := &handler.TableMetadata{
			Name: "users", NCols: 2,
			Cols: []*dictionary.ColumnMeta{
				{Name: "id", Pos: 0}, {Name: "name", Pos: 1},
			},
		}

		// WHEN
		_, err := resolveSelectColumns(
			[]ast.ColumnId{{ColName: "nonexistent"}},
			[]*handler.TableMetadata{tbl},
		)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestResolveSelectColumnsForJoin(t *testing.T) {
	// GIVEN: users(id, name) + orders(id, user_id) の結合カラム
	joinedCols := []joinedColumn{
		{tableName: "users", colName: "id", pos: 0},
		{tableName: "users", colName: "name", pos: 1},
		{tableName: "orders", colName: "id", pos: 2},
		{tableName: "orders", colName: "user_id", pos: 3},
	}

	t.Run("SELECT * で全カラムの位置が返される", func(t *testing.T) {
		// WHEN
		pos, err := resolveSelectColumnsForJoin(nil, joinedCols, 4)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, []uint16{0, 1, 2, 3}, pos)
	})

	t.Run("修飾名で特定カラムの位置が解決される", func(t *testing.T) {
		// WHEN: orders.user_id と users.name を指定
		pos, err := resolveSelectColumnsForJoin(
			[]ast.ColumnId{
				{TableName: "orders", ColName: "user_id"},
				{TableName: "users", ColName: "name"},
			},
			joinedCols, 4,
		)

		// THEN: カラム位置が指定順に返される
		require.NoError(t, err)
		assert.Equal(t, []uint16{3, 1}, pos)
	})

	t.Run("非修飾名で曖昧なカラムはエラーになる", func(t *testing.T) {
		// WHEN: "id" は users と orders の両方にある
		_, err := resolveSelectColumnsForJoin(
			[]ast.ColumnId{{ColName: "id"}},
			joinedCols, 4,
		)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ambiguous")
	})

	t.Run("非修飾名で一意のカラムは解決される", func(t *testing.T) {
		// WHEN: "user_id" は orders にのみ存在
		pos, err := resolveSelectColumnsForJoin(
			[]ast.ColumnId{{ColName: "user_id"}},
			joinedCols, 4,
		)

		// THEN
		require.NoError(t, err)
		assert.Equal(t, []uint16{3}, pos)
	})
}

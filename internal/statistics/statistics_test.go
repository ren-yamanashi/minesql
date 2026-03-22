package statistics_test

import (
	"minesql/internal/ast"
	"minesql/internal/engine"
	"minesql/internal/planner"
	"minesql/internal/statistics"
	"testing"

	"github.com/stretchr/testify/assert"
)

// executePlan は AST を planner に渡して実行し、全レコードを返す
func executePlan(t *testing.T, stmt ast.Statement) {
	t.Helper()
	exec, err := planner.Start(stmt)
	assert.NoError(t, err)

	for {
		record, err := exec.Next()
		assert.NoError(t, err)
		if record == nil {
			return
		}
	}
}

// setupStatisticsTable はストレージを初期化し、統計情報テスト用のテーブルを作成する
//
// テーブル: products (id, name, category)
//   - プライマリキー: id
//   - ユニークインデックス: name
//
// 3 レコードを挿入する:
//
//	| id  | name   | category |
//	| --- | ------ | -------- |
//	| 1   | Apple  | Fruit    |
//	| 2   | Banana | Fruit    |
//	| 3   | Carrot | Veggie   |
//
// 期待される統計値:
//   - R(T) = 3
//   - V(T, id) = 3, V(T, name) = 3, V(T, category) = 2
//   - min(id) = "1", max(id) = "3"
//   - min(name) = "Apple", max(name) = "Carrot"
//   - min(category) = "Fruit", max(category) = "Veggie"
func setupStatisticsTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	executePlan(t, &ast.CreateTableStmt{
		StmtType:  ast.StmtTypeCreate,
		Keyword:   ast.KeywordTable,
		TableName: "products",
		CreateDefinitions: []ast.Definition{
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "name", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "category", DataType: ast.DataTypeVarchar},
			&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
				*ast.NewColumnId("id"),
			}},
			&ast.ConstraintUniqueKeyDef{DefType: ast.DefTypeConstraintUniqueKey, Column: *ast.NewColumnId("name")},
		},
	})

	executePlan(t, &ast.InsertStmt{
		StmtType: ast.StmtTypeInsert,
		Table:    *ast.NewTableId("products"),
		Cols: []ast.ColumnId{
			*ast.NewColumnId("id"),
			*ast.NewColumnId("name"),
			*ast.NewColumnId("category"),
		},
		Values: [][]ast.Literal{
			{
				ast.NewStringLiteral("1", "1"),
				ast.NewStringLiteral("Apple", "Apple"),
				ast.NewStringLiteral("Fruit", "Fruit"),
			},
			{
				ast.NewStringLiteral("2", "2"),
				ast.NewStringLiteral("Banana", "Banana"),
				ast.NewStringLiteral("Fruit", "Fruit"),
			},
			{
				ast.NewStringLiteral("3", "3"),
				ast.NewStringLiteral("Carrot", "Carrot"),
				ast.NewStringLiteral("Veggie", "Veggie"),
			},
		},
	})
}

// setupEmptyTable はストレージを初期化し、データなしの空テーブルを作成する
func setupEmptyTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	executePlan(t, &ast.CreateTableStmt{
		StmtType:  ast.StmtTypeCreate,
		Keyword:   ast.KeywordTable,
		TableName: "items",
		CreateDefinitions: []ast.Definition{
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "id", DataType: ast.DataTypeVarchar},
			&ast.ColumnDef{DefType: ast.DefTypeColumn, ColName: "name", DataType: ast.DataTypeVarchar},
			&ast.ConstraintPrimaryKeyDef{DefType: ast.DefTypeConstraintPrimaryKey, Columns: []ast.ColumnId{
				*ast.NewColumnId("id"),
			}},
		},
	})
}

func TestAnalyze(t *testing.T) {
	t.Run("レコード数が正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := statistics.NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: R(T) = 3
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.RecordCount)
	})

	t.Run("リーフページ数が正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコードは 1 ページに収まる
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := statistics.NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: P(T) = 1
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), result.LeafPageCount)
	})

	t.Run("各カラムのユニーク値数が正しく算出される", func(t *testing.T) {
		// GIVEN: id は全件異なる、name は全件異なる、category は "Fruit" が重複
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := statistics.NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.ColumnStats["id"].UniqueValues)       // V(T, id) = 3
		assert.Equal(t, uint64(3), result.ColumnStats["name"].UniqueValues)     // V(T, name) = 3
		assert.Equal(t, uint64(2), result.ColumnStats["category"].UniqueValues) // V(T, category) = 2
	})

	t.Run("各カラムの min/max が正しく算出される", func(t *testing.T) {
		// GIVEN
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := statistics.NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN
		assert.NoError(t, err)

		assert.Equal(t, []byte("1"), result.ColumnStats["id"].MinValue)
		assert.Equal(t, []byte("3"), result.ColumnStats["id"].MaxValue)

		assert.Equal(t, []byte("Apple"), result.ColumnStats["name"].MinValue)
		assert.Equal(t, []byte("Carrot"), result.ColumnStats["name"].MaxValue)

		assert.Equal(t, []byte("Fruit"), result.ColumnStats["category"].MinValue)
		assert.Equal(t, []byte("Veggie"), result.ColumnStats["category"].MaxValue)
	})

	t.Run("セカンダリインデックスの高さが正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコードでは B+Tree の高さは 1 (ルートリーフのみ)
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := statistics.NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: ユニークインデックスが 1 つ、高さ 1
		assert.NoError(t, err)
		assert.Len(t, result.SecondaryIndexStats, 1)
		for _, idxStat := range result.SecondaryIndexStats {
			assert.Equal(t, uint64(1), idxStat.Height)
		}
	})

	t.Run("INSERT 後に再 Analyze するとレコード数やユニーク値数が増加する", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := statistics.NewStatistics(meta, eng.BufferPool)

		before, err := stats.Analyze()
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), before.RecordCount)
		assert.Equal(t, uint64(2), before.ColumnStats["category"].UniqueValues)

		// WHEN: 新しいカテゴリを持つレコードを追加
		executePlan(t, &ast.InsertStmt{
			StmtType: ast.StmtTypeInsert,
			Table:    *ast.NewTableId("products"),
			Cols: []ast.ColumnId{
				*ast.NewColumnId("id"),
				*ast.NewColumnId("name"),
				*ast.NewColumnId("category"),
			},
			Values: [][]ast.Literal{
				{
					ast.NewStringLiteral("4", "4"),
					ast.NewStringLiteral("Donut", "Donut"),
					ast.NewStringLiteral("Snack", "Snack"),
				},
			},
		})

		after, err := stats.Analyze()

		// THEN: R(T) が 3 -> 4 に増加
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), after.RecordCount)
		// V(T, category) が 2 -> 3 に増加 ("Snack" が追加)
		assert.Equal(t, uint64(3), after.ColumnStats["category"].UniqueValues)
		// V(T, name) が 3 -> 4 に増加
		assert.Equal(t, uint64(4), after.ColumnStats["name"].UniqueValues)
		// max(name) が "Carrot" -> "Donut" に変化
		assert.Equal(t, []byte("Donut"), after.ColumnStats["name"].MaxValue)
	})

	t.Run("DELETE 後に再 Analyze するとレコード数やユニーク値数が減少する", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := statistics.NewStatistics(meta, eng.BufferPool)

		before, err := stats.Analyze()
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), before.RecordCount)
		assert.Equal(t, uint64(3), before.ColumnStats["name"].UniqueValues)

		// WHEN: "Carrot" (唯一の "Veggie") を削除
		executePlan(t, &ast.DeleteStmt{
			StmtType: ast.StmtTypeDelete,
			From:     *ast.NewTableId("products"),
			Where: &ast.WhereClause{
				Condition: ast.NewBinaryExpr(
					"=",
					ast.NewLhsColumn(*ast.NewColumnId("id")),
					ast.NewRhsLiteral(ast.NewStringLiteral("3", "3")),
				),
				IsSet: true,
			},
		})

		after, err := stats.Analyze()

		// THEN: R(T) が 3 -> 2 に減少
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), after.RecordCount)
		// V(T, name) が 3 -> 2 に減少
		assert.Equal(t, uint64(2), after.ColumnStats["name"].UniqueValues)
		// V(T, category) が 2 -> 1 に減少 ("Veggie" がなくなる)
		assert.Equal(t, uint64(1), after.ColumnStats["category"].UniqueValues)
		// max(name) が "Carrot" -> "Banana" に変化
		assert.Equal(t, []byte("Banana"), after.ColumnStats["name"].MaxValue)
	})

	t.Run("空テーブルではレコード数 0 でカラム統計も空になる", func(t *testing.T) {
		// GIVEN: テーブルを作成するがデータは挿入しない
		setupEmptyTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("items")
		assert.True(t, ok)
		stats := statistics.NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), result.RecordCount)
		assert.Equal(t, uint64(1), result.LeafPageCount)
		assert.Empty(t, result.SecondaryIndexStats)

		// カラム統計は存在するがユニーク値数は 0
		for _, colStat := range result.ColumnStats {
			assert.Equal(t, uint64(0), colStat.UniqueValues)
		}
	})
}

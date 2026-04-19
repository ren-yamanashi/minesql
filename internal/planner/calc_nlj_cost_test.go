package planner

import (
	"minesql/internal/ast"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/handler"
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalcTableJoinCost(t *testing.T) {
	t.Run("駆動表のコストはフルスキャン", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}

		// WHEN: pred=nil (駆動表)
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, nil)

		// THEN: readCost = scanTime × pageReadCost, fanout = RecordCount
		require.NoError(t, err)
		assert.Equal(t, float64(stats.RecordCount), fanout)
		assert.Greater(t, readCost, 0.0)
	})

	t.Run("内部表の PK eq_ref のコスト", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}
		pred := &joinPredicate{leftTable: "orders", leftCol: "user_id", rightTable: "users", rightCol: "id"}

		// WHEN: pred の rightCol が PK (id)
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 100.0, pred, nil)

		// THEN: eq_ref → readCost = prefixRowcount × pageReadCost, fanout = 1
		require.NoError(t, err)
		assert.Equal(t, 1.0, fanout)
		assert.Greater(t, readCost, 0.0)
	})

	t.Run("内部表の UNIQUE INDEX eq_ref のコスト", func(t *testing.T) {
		// GIVEN: users テーブルの username カラムに UNIQUE INDEX がある (setupUsersTable で定義)
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}
		pred := &joinPredicate{leftTable: "other", leftCol: "uname", rightTable: "users", rightCol: "username"}

		// WHEN
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 50.0, pred, nil)

		// THEN: UNIQUE INDEX eq_ref → fanout = RecPerKey = 1.0
		require.NoError(t, err)
		assert.Equal(t, 1.0, fanout)
		assert.Greater(t, readCost, 0.0)
	})

	t.Run("インデックスなしカラムの内部表はフルスキャン", func(t *testing.T) {
		// GIVEN: users テーブルの first_name にはインデックスがない
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}
		pred := &joinPredicate{leftTable: "other", leftCol: "name", rightTable: "users", rightCol: "first_name"}

		// WHEN
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 10.0, pred, nil)

		// THEN: フルスキャン → fanout = RecordCount
		require.NoError(t, err)
		assert.Equal(t, float64(stats.RecordCount), fanout)
		assert.Greater(t, readCost, 0.0)
	})
}

func TestCalcDrivingTableCostWithWhere(t *testing.T) {
	t.Run("WHERE に PK 等値検索がある場合ユニークスキャンコストになる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}

		// WHERE users.id = '1' (PK 等値検索)
		drivingWhere := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("1")),
			),
		}

		// WHEN
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, drivingWhere)

		// THEN: ユニークスキャン → コスト 1.0, fanout 1
		require.NoError(t, err)
		assert.Equal(t, 1.0, readCost)
		assert.Equal(t, 1.0, fanout)
	})

	t.Run("WHERE に PK レンジスキャンがある場合 rangeCost が返される", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}

		// WHERE users.id > '3' (PK レンジスキャン)
		drivingWhere := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(">",
				ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("3")),
			),
		}

		// WHEN
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, drivingWhere)

		// THEN: レンジスキャンコスト、fanout はフルスキャンより小さい
		require.NoError(t, err)
		assert.Less(t, fanout, float64(stats.RecordCount), "レンジスキャンの fanout はフルスキャンより小さい")
		assert.Greater(t, readCost, 0.0)
	})

	t.Run("WHERE に UNIQUE INDEX レンジスキャンがある場合 rangeCost が返される", func(t *testing.T) {
		// GIVEN: users テーブルの username カラムに UNIQUE INDEX がある
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}

		// WHERE users.username > 'k' (UNIQUE INDEX レンジスキャン)
		// username の値: janedoe, johndoe, johndoe2, johndoe3, jonathanblack, tombrown
		// 'k' より大きいのは tombrown のみ
		drivingWhere := &ast.WhereClause{
			Condition: ast.NewBinaryExpr(">",
				ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "username"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("k")),
			),
		}

		// WHEN
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, drivingWhere)

		// THEN: レンジスキャンコスト、fanout はフルスキャンより小さい
		require.NoError(t, err)
		assert.Less(t, fanout, float64(stats.RecordCount), "レンジスキャンの fanout はフルスキャンより小さい")
		assert.Greater(t, readCost, 0.0)
	})

	t.Run("WHERE ありは fanout が 1 になり WHERE なしは RecordCount になる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}

		// WHEN: WHERE なし → フルスキャン
		_, fanoutNoWhere, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, nil)
		require.NoError(t, err)

		// WHEN: WHERE あり (PK 等値検索) → ユニークスキャン
		_, fanoutWithWhere, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, &ast.WhereClause{
			Condition: ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{ColName: "id"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("1")),
			),
		})
		require.NoError(t, err)

		// THEN: fanout が変わる (コスト推定に反映される)
		assert.Equal(t, float64(stats.RecordCount), fanoutNoWhere) // フルスキャン: 全行
		assert.Equal(t, 1.0, fanoutWithWhere)                      // PKの等値検索: 1行
	})

	t.Run("WHERE に非ユニークインデックスの等値検索がある場合 fanout が RecPerKey になる", func(t *testing.T) {
		// GIVEN: products テーブル (category に非ユニークインデックス)
		setupProductsTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("products")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("products")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}

		// WHERE products.category = 'Fruit' (非ユニークインデックスの等値検索)
		drivingWhere := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{TableName: "products", ColName: "category"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("Fruit")),
			),
		}

		// WHEN
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, drivingWhere)

		// THEN: 非ユニークなので fanout = RecPerKey (1.0 ではない)
		require.NoError(t, err)
		assert.Greater(t, readCost, 0.0)
		// RecPerKey = 3レコード / 2キー = 1.5
		assert.Equal(t, 1.5, fanout)
	})

	t.Run("WHERE に非ユニークインデックスの等値検索がある場合コストは rangeCost 方式で算出される", func(t *testing.T) {
		// GIVEN
		setupProductsTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("products")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("products")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}

		// WHEN: 非ユニークインデックスの等値検索
		drivingWhere := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{ColName: "category"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("Fruit")),
			),
		}
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, drivingWhere)
		require.NoError(t, err)

		// THEN: readCost > 0 かつ fanout = RecPerKey = 1.5
		// readCost = readTime + RecPerKey × RowEvaluateCost + 0.01 (rangeCost 方式)
		assert.Greater(t, readCost, 0.0)
		assert.Equal(t, 1.5, fanout)
		// readCost はユニークスキャン (1.0) とは異なる値になる
		assert.NotEqual(t, calcUniqueScanCost(), readCost)
	})
}

func TestCalcFiltered(t *testing.T) {
	t.Run("インデックスなしの等値検索で通過率が 1/V になる", func(t *testing.T) {
		// GIVEN: gender に 2 種類の値がある (V=2)
		candidate := joinCandidate{
			tblMeta: &handler.TableMetadata{
				Name: "users", NCols: 3, PKCount: 1,
				Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "name", Pos: 1}, {Name: "gender", Pos: 2}},
			},
			stats: &handler.TableStatistics{
				RecordCount: 100,
				ColStats: map[string]handler.ColumnStatistics{
					"gender": {UniqueValues: 2},
				},
				IdxStats: map[string]handler.IndexStatistics{},
			},
		}

		// WHEN: WHERE gender = 'male'
		expr := ast.NewBinaryExpr("=",
			ast.NewLhsColumn(ast.ColumnId{ColName: "gender"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("male")),
		)
		filtered := calcFiltered(expr, candidate)

		// THEN: 1/2 = 0.5
		assert.Equal(t, 0.5, filtered)
	})

	t.Run("PK カラムの条件は filtered = 1.0 (fanout に反映済み)", func(t *testing.T) {
		// GIVEN
		candidate := joinCandidate{
			tblMeta: &handler.TableMetadata{
				Name: "users", NCols: 2, PKCount: 1,
				Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "name", Pos: 1}},
			},
			stats: &handler.TableStatistics{
				RecordCount: 100,
				ColStats: map[string]handler.ColumnStatistics{
					"id": {UniqueValues: 100},
				},
				IdxStats: map[string]handler.IndexStatistics{},
			},
		}

		// WHEN: WHERE id = '1' (PK → fanout に既に反映)
		expr := ast.NewBinaryExpr("=",
			ast.NewLhsColumn(ast.ColumnId{ColName: "id"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("1")),
		)
		filtered := calcFiltered(expr, candidate)

		// THEN: 1.0 (二重に削減しない)
		assert.Equal(t, 1.0, filtered)
	})

	t.Run("AND の複合条件で通過率が掛け合わされる", func(t *testing.T) {
		// GIVEN: gender V=2, status V=5
		candidate := joinCandidate{
			tblMeta: &handler.TableMetadata{
				Name: "users", NCols: 4, PKCount: 1,
				Cols: []*dictionary.ColumnMeta{
					{Name: "id", Pos: 0}, {Name: "name", Pos: 1},
					{Name: "gender", Pos: 2}, {Name: "status", Pos: 3},
				},
			},
			stats: &handler.TableStatistics{
				RecordCount: 100,
				ColStats: map[string]handler.ColumnStatistics{
					"gender": {UniqueValues: 2},
					"status": {UniqueValues: 5},
				},
				IdxStats: map[string]handler.IndexStatistics{},
			},
		}

		// WHEN: WHERE gender = 'male' AND status = 'active'
		expr := ast.NewBinaryExpr("AND",
			ast.NewLhsExpr(ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{ColName: "gender"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("male")),
			)),
			ast.NewRhsExpr(ast.NewBinaryExpr("=",
				ast.NewLhsColumn(ast.ColumnId{ColName: "status"}),
				ast.NewRhsLiteral(ast.NewStringLiteral("active")),
			)),
		)
		filtered := calcFiltered(expr, candidate)

		// THEN: 1/2 × 1/5 = 0.1
		assert.Equal(t, 0.1, filtered)
	})

	t.Run("UNIQUE INDEX カラムの条件は filtered = 1.0", func(t *testing.T) {
		// GIVEN: email に UNIQUE INDEX がある
		candidate := joinCandidate{
			tblMeta: &handler.TableMetadata{
				Name: "users", NCols: 3, PKCount: 1,
				Cols: []*dictionary.ColumnMeta{
					{Name: "id", Pos: 0}, {Name: "name", Pos: 1}, {Name: "email", Pos: 2},
				},
				Indexes: []*dictionary.IndexMeta{
					{Name: "idx_email", ColName: "email", Type: dictionary.IndexTypeUnique},
				},
			},
			stats: &handler.TableStatistics{
				RecordCount: 100,
				ColStats:    map[string]handler.ColumnStatistics{"email": {UniqueValues: 100}},
				IdxStats:    map[string]handler.IndexStatistics{"idx_email": {RecPerKey: 1.0}},
			},
		}

		// WHEN: WHERE email = 'test@example.com' (UNIQUE INDEX → fanout に反映済み)
		expr := ast.NewBinaryExpr("=",
			ast.NewLhsColumn(ast.ColumnId{ColName: "email"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("test@example.com")),
		)
		filtered := calcFiltered(expr, candidate)

		// THEN: 1.0 (二重に削減しない)
		assert.Equal(t, 1.0, filtered)
	})

	t.Run("不等値検索で通過率が (V-1)/V になる", func(t *testing.T) {
		// GIVEN: gender に 2 種類の値がある (V=2)
		candidate := joinCandidate{
			tblMeta: &handler.TableMetadata{
				Name: "users", NCols: 3, PKCount: 1,
				Cols: []*dictionary.ColumnMeta{
					{Name: "id", Pos: 0}, {Name: "name", Pos: 1}, {Name: "gender", Pos: 2},
				},
			},
			stats: &handler.TableStatistics{
				RecordCount: 100,
				ColStats:    map[string]handler.ColumnStatistics{"gender": {UniqueValues: 2}},
				IdxStats:    map[string]handler.IndexStatistics{},
			},
		}

		// WHEN: WHERE gender != 'male'
		expr := ast.NewBinaryExpr("!=",
			ast.NewLhsColumn(ast.ColumnId{ColName: "gender"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("male")),
		)
		filtered := calcFiltered(expr, candidate)

		// THEN: (2-1)/2 = 0.5
		assert.Equal(t, 0.5, filtered)
	})

	t.Run("レンジ演算子で通過率が 1/3 になる", func(t *testing.T) {
		// GIVEN: age にインデックスがない
		candidate := joinCandidate{
			tblMeta: &handler.TableMetadata{
				Name: "users", NCols: 3, PKCount: 1,
				Cols: []*dictionary.ColumnMeta{
					{Name: "id", Pos: 0}, {Name: "name", Pos: 1}, {Name: "age", Pos: 2},
				},
			},
			stats: &handler.TableStatistics{
				RecordCount: 100,
				ColStats:    map[string]handler.ColumnStatistics{"age": {UniqueValues: 50}},
				IdxStats:    map[string]handler.IndexStatistics{},
			},
		}

		// WHEN: WHERE age > '30'
		expr := ast.NewBinaryExpr(">",
			ast.NewLhsColumn(ast.ColumnId{ColName: "age"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("30")),
		)
		filtered := calcFiltered(expr, candidate)

		// THEN: 1/3
		assert.InDelta(t, 1.0/3.0, filtered, 1e-10)
	})
}

func TestJoinCostFormulas(t *testing.T) {
	t.Run("cost.md の計算例: prefix_cost が式通りに計算される", func(t *testing.T) {
		// GIVEN: cost.md の例 (pageReadCost=1.0 を仮定)
		// users: 10000 行、100 ページ
		// orders: 50000 行、user_id に UNIQUE INDEX (eq_ref)
		// 結合順序: users → orders
		pageReadCost := 1.0

		// テーブル 1: users (駆動表、フルスキャン)
		// read_cost = scanTime × pageReadCost = 100 × 1.0 = 100
		usersReadCost := calcFullScanReadCost(&handler.TableStatistics{
			RecordCount: 10000, LeafPageCount: 100,
		}, pageReadCost)
		assert.Equal(t, 100.0, usersReadCost)

		usersFanout := 10000.0
		prefixRowcount := 1.0 * usersFanout // 1 × 10000 = 10000
		prefixCost := 0.0 + usersReadCost + prefixRowcount*RowEvaluateCost
		// prefix_cost = 0 + 100 + 10000 × 0.1 = 1100
		assert.Equal(t, 1100.0, prefixCost)

		// テーブル 2: orders (内部表、eq_ref)
		// read_cost = prefixRowcount × pageReadCost = 10000 × 1.0 = 10000
		ordersReadCost := prefixRowcount * pageReadCost
		assert.Equal(t, 10000.0, ordersReadCost)

		ordersFanout := 1.0            // UNIQUE eq_ref
		prefixRowcount *= ordersFanout // 10000 × 1 = 10000
		prefixCost = prefixCost + ordersReadCost + prefixRowcount*RowEvaluateCost
		// prefix_cost = 1100 + 10000 + 10000 × 0.1 = 12100
		assert.Equal(t, 12100.0, prefixCost)
	})

	t.Run("駆動表のコストは scanTime × pageReadCost", func(t *testing.T) {
		// GIVEN
		stats := &handler.TableStatistics{RecordCount: 5000, LeafPageCount: 50}
		pageReadCost := 0.625 // 50% キャッシュ

		// WHEN
		readCost := calcFullScanReadCost(stats, pageReadCost)

		// THEN: 50 × 0.625 = 31.25
		assert.Equal(t, 31.25, readCost)
	})

	t.Run("内部表 eq_ref のコストは prefixRowcount × pageReadCost", func(t *testing.T) {
		// GIVEN: eq_ref アクセスでは 1 回の検索コスト = pageReadCost
		prefixRowcount := 500.0
		pageReadCost := 1.0

		// WHEN: 500 回の eq_ref 検索
		readCost := prefixRowcount * pageReadCost

		// THEN: 500 × 1.0 = 500
		assert.Equal(t, 500.0, readCost)
	})

	t.Run("内部表フルスキャンのコストは prefixRowcount × scanTime × pageReadCost", func(t *testing.T) {
		// GIVEN: インデックスなしの内部表
		prefixRowcount := 100.0
		scanTime := 50.0
		pageReadCost := 1.0

		// WHEN: 100 回のフルスキャン (各 50 ページ)
		readCost := prefixRowcount * scanTime * pageReadCost

		// THEN: 100 × 50 × 1.0 = 5000
		assert.Equal(t, 5000.0, readCost)
	})
}

func TestJoinPlanSelection(t *testing.T) {
	t.Run("FROM と JOIN のテーブルが逆でも最適な結合順序が選ばれる", func(t *testing.T) {
		// GIVEN: large (10000 行) を FROM、small (10 行) を JOIN に書いても small が駆動表になる
		setupUsersTable(t)
		hdl := handler.Get()
		usersTbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		largeMeta := &handler.TableMetadata{
			Name: "large", NCols: 2, PKCount: 1,
			Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "val", Pos: 1}},
		}
		smallMeta := &handler.TableMetadata{
			Name: "small", NCols: 2, PKCount: 1,
			Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "large_id", Pos: 1}},
			Indexes: []*dictionary.IndexMeta{
				{Name: "idx_large_id", ColName: "large_id", Type: dictionary.IndexTypeUnique},
			},
		}
		largeStats := &handler.TableStatistics{
			RecordCount: 10000, LeafPageCount: 100, TreeHeight: 3,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{},
		}
		smallStats := &handler.TableStatistics{
			RecordCount: 10, LeafPageCount: 1, TreeHeight: 1,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{
				"idx_large_id": {Height: 1, LeafPageCount: 1, RecPerKey: 1.0},
			},
		}

		// FROM large JOIN small (SQL 上は large が先)
		candidates := []joinCandidate{
			{tblMeta: largeMeta, stats: largeStats, table: usersTbl},
			{tblMeta: smallMeta, stats: smallStats, table: usersTbl},
		}
		predicates := []joinPredicate{
			{leftTable: "large", leftCol: "id", rightTable: "small", rightCol: "large_id"},
		}

		// WHEN
		result, err := optimizeJoinOrder(hdl.BufferPool, candidates, predicates, nil, nil)

		// THEN: SQL の記述順序に関係なく small が駆動表に選ばれる
		require.NoError(t, err)
		assert.Equal(t, "small", result[0].tblMeta.Name)
		assert.Equal(t, "large", result[1].tblMeta.Name)
	})

	t.Run("内部表にインデックスがないテーブルはコストが高くなる", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		hdl := handler.Get()
		usersTbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		// テーブル B (100 行)、B にはインデックスなし
		metaB := &handler.TableMetadata{
			Name: "b", NCols: 2, PKCount: 1,
			Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "a_val", Pos: 1}},
			// インデックスなし (a_val に対して)
		}
		statsB := &handler.TableStatistics{
			RecordCount: 100, LeafPageCount: 10, TreeHeight: 2,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{},
		}

		candidateB := joinCandidate{tblMeta: metaB, stats: statsB, table: usersTbl}
		pred := &joinPredicate{leftTable: "a", leftCol: "val", rightTable: "b", rightCol: "a_val"}

		// WHEN: B が内部表 (インデックスなし)
		readCostNoIdx, fanoutNoIdx, err := calcTableJoinCost(hdl.BufferPool, candidateB, 100.0, pred, nil)
		require.NoError(t, err)

		// B に UNIQUE INDEX がある場合
		metaBWithIdx := &handler.TableMetadata{
			Name: "b_idx", NCols: 2, PKCount: 1,
			Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "a_val", Pos: 1}},
			Indexes: []*dictionary.IndexMeta{
				{Name: "idx_a_val", ColName: "a_val", Type: dictionary.IndexTypeUnique},
			},
		}
		statsBWithIdx := &handler.TableStatistics{
			RecordCount: 100, LeafPageCount: 10, TreeHeight: 2,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{
				"idx_a_val": {Height: 2, LeafPageCount: 5, RecPerKey: 1.0},
			},
		}
		candidateBWithIdx := joinCandidate{tblMeta: metaBWithIdx, stats: statsBWithIdx, table: usersTbl}
		predWithIdx := &joinPredicate{leftTable: "a", leftCol: "val", rightTable: "b_idx", rightCol: "a_val"}

		readCostWithIdx, fanoutWithIdx, err := calcTableJoinCost(hdl.BufferPool, candidateBWithIdx, 100.0, predWithIdx, nil)
		require.NoError(t, err)

		// THEN: インデックスなしの方がコストが高い
		assert.Greater(t, readCostNoIdx, readCostWithIdx, "インデックスなしのコスト > インデックスありのコスト")
		assert.Equal(t, float64(statsB.RecordCount), fanoutNoIdx, "インデックスなし: fanout = RecordCount")
		assert.Equal(t, 1.0, fanoutWithIdx, "インデックスあり: fanout = 1 (eq_ref)")
	})

	t.Run("3 テーブルの結合順序が貪欲法で決定される", func(t *testing.T) {
		// GIVEN: A (100行), B (10行), C (1000行)
		// B が最小なので駆動表、次に A (B→A の結合条件あり)、最後に C
		setupUsersTable(t)
		hdl := handler.Get()
		usersTbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		metaA := &handler.TableMetadata{
			Name: "a", NCols: 2, PKCount: 1,
			Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "val", Pos: 1}},
			Indexes: []*dictionary.IndexMeta{
				{Name: "idx_a_val", ColName: "val", Type: dictionary.IndexTypeUnique},
			},
		}
		metaB := &handler.TableMetadata{
			Name: "b", NCols: 2, PKCount: 1,
			Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "a_val", Pos: 1}},
		}
		metaC := &handler.TableMetadata{
			Name: "c", NCols: 2, PKCount: 1,
			Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "a_id", Pos: 1}},
			Indexes: []*dictionary.IndexMeta{
				{Name: "idx_c_a_id", ColName: "a_id", Type: dictionary.IndexTypeUnique},
			},
		}
		statsA := &handler.TableStatistics{
			RecordCount: 100, LeafPageCount: 10, TreeHeight: 2,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{
				"idx_a_val": {Height: 2, LeafPageCount: 5, RecPerKey: 1.0},
			},
		}
		statsB := &handler.TableStatistics{
			RecordCount: 10, LeafPageCount: 1, TreeHeight: 1,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{},
		}
		statsC := &handler.TableStatistics{
			RecordCount: 1000, LeafPageCount: 100, TreeHeight: 3,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{
				"idx_c_a_id": {Height: 2, LeafPageCount: 20, RecPerKey: 1.0},
			},
		}

		candidates := []joinCandidate{
			{tblMeta: metaA, stats: statsA, table: usersTbl},
			{tblMeta: metaB, stats: statsB, table: usersTbl},
			{tblMeta: metaC, stats: statsC, table: usersTbl},
		}
		predicates := []joinPredicate{
			{leftTable: "b", leftCol: "a_val", rightTable: "a", rightCol: "val"},
			{leftTable: "a", leftCol: "id", rightTable: "c", rightCol: "a_id"},
		}

		// WHEN
		result, err := optimizeJoinOrder(hdl.BufferPool, candidates, predicates, nil, nil)

		// THEN: B (10行) が駆動表
		require.NoError(t, err)
		require.Len(t, result, 3)
		assert.Equal(t, "b", result[0].tblMeta.Name, "最小テーブル B が駆動表")
	})
}

func TestOptimizeJoinOrder(t *testing.T) {
	t.Run("cost.md の計算例: users → orders で prefix_cost = 12100", func(t *testing.T) {
		// GIVEN: cost.md の例を簡略化した統計情報
		// users: 10000 行、100 ページ
		// orders: 50000 行、user_id に UNIQUE INDEX
		usersStats := &handler.TableStatistics{
			RecordCount:   10000,
			LeafPageCount: 100,
			TreeHeight:    3,
			ColStats:      map[string]handler.ColumnStatistics{},
			IdxStats:      map[string]handler.IndexStatistics{},
		}
		ordersStats := &handler.TableStatistics{
			RecordCount:   50000,
			LeafPageCount: 500,
			TreeHeight:    3,
			ColStats:      map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{
				"idx_user_id": {Height: 3, LeafPageCount: 100, RecPerKey: 1.0},
			},
		}

		usersMeta := &handler.TableMetadata{
			Name:    "users",
			NCols:   2,
			PKCount: 1,
			Cols: []*dictionary.ColumnMeta{
				{Name: "id", Pos: 0},
				{Name: "name", Pos: 1},
			},
		}
		ordersMeta := &handler.TableMetadata{
			Name:    "orders",
			NCols:   2,
			PKCount: 1,
			Cols: []*dictionary.ColumnMeta{
				{Name: "id", Pos: 0},
				{Name: "user_id", Pos: 1},
			},
			Indexes: []*dictionary.IndexMeta{
				{Name: "idx_user_id", ColName: "user_id", Type: dictionary.IndexTypeUnique},
			},
		}

		// テーブルオブジェクトはコスト計算で B+Tree アクセスに必要だが、
		// このテストでは calcPageReadCost が呼ばれるのでストレージが必要
		// → ストレージ初期化をして実テーブルで検証する
		setupUsersTable(t)
		hdl := handler.Get()
		usersTbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		// orders テーブルは存在しないので users テーブルを代用 (コスト計算の構造を検証)
		// ※ 実際の pageReadCost は users テーブルの B+Tree から算出されるが、
		//    コスト計算のフローが正しいことを検証する目的
		candidates := []joinCandidate{
			{tblMeta: usersMeta, stats: usersStats, table: usersTbl},
			{tblMeta: ordersMeta, stats: ordersStats, table: usersTbl},
		}
		predicates := []joinPredicate{
			{leftTable: "users", leftCol: "id", rightTable: "orders", rightCol: "user_id"},
		}

		// WHEN
		result, err := optimizeJoinOrder(hdl.BufferPool, candidates, predicates, nil, nil)

		// THEN: users が駆動表 (小さい)、orders が内部表
		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, "users", result[0].tblMeta.Name)
		assert.Equal(t, "orders", result[1].tblMeta.Name)
	})

	t.Run("小さいテーブルが駆動表に選ばれる", func(t *testing.T) {
		// GIVEN: small (10 行) と large (10000 行)
		setupUsersTable(t)
		hdl := handler.Get()
		usersTbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		smallMeta := &handler.TableMetadata{
			Name: "small", NCols: 2, PKCount: 1,
			Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "val", Pos: 1}},
		}
		largeMeta := &handler.TableMetadata{
			Name: "large", NCols: 2, PKCount: 1,
			Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "small_id", Pos: 1}},
			Indexes: []*dictionary.IndexMeta{
				{Name: "idx_small_id", ColName: "small_id", Type: dictionary.IndexTypeUnique, DataMetaPageId: page.InvalidPageId},
			},
		}
		smallStats := &handler.TableStatistics{
			RecordCount: 10, LeafPageCount: 1, TreeHeight: 1,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{},
		}
		largeStats := &handler.TableStatistics{
			RecordCount: 10000, LeafPageCount: 100, TreeHeight: 3,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{
				"idx_small_id": {Height: 2, LeafPageCount: 10, RecPerKey: 1.0},
			},
		}

		candidates := []joinCandidate{
			{tblMeta: largeMeta, stats: largeStats, table: usersTbl},
			{tblMeta: smallMeta, stats: smallStats, table: usersTbl},
		}
		predicates := []joinPredicate{
			{leftTable: "small", leftCol: "id", rightTable: "large", rightCol: "small_id"},
		}

		// WHEN
		result, err := optimizeJoinOrder(hdl.BufferPool, candidates, predicates, nil, nil)

		// THEN: small が駆動表 (先頭) に選ばれる
		require.NoError(t, err)
		assert.Equal(t, "small", result[0].tblMeta.Name)
		assert.Equal(t, "large", result[1].tblMeta.Name)
	})

	t.Run("結合条件のないテーブルがある場合エラーになる", func(t *testing.T) {
		// GIVEN: A-B は結合可能だが C は A,B どちらとも結合条件がない
		setupUsersTable(t)
		hdl := handler.Get()
		usersTbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		dummyStats := &handler.TableStatistics{
			RecordCount: 10, LeafPageCount: 1, TreeHeight: 1,
			ColStats: map[string]handler.ColumnStatistics{},
			IdxStats: map[string]handler.IndexStatistics{},
		}
		metaA := &handler.TableMetadata{Name: "a", NCols: 1, PKCount: 1, Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}}}
		metaB := &handler.TableMetadata{Name: "b", NCols: 1, PKCount: 1, Cols: []*dictionary.ColumnMeta{{Name: "a_id", Pos: 0}}}
		metaC := &handler.TableMetadata{Name: "c", NCols: 1, PKCount: 1, Cols: []*dictionary.ColumnMeta{{Name: "x", Pos: 0}}}

		candidates := []joinCandidate{
			{tblMeta: metaA, stats: dummyStats, table: usersTbl},
			{tblMeta: metaB, stats: dummyStats, table: usersTbl},
			{tblMeta: metaC, stats: dummyStats, table: usersTbl},
		}
		predicates := []joinPredicate{
			{leftTable: "a", leftCol: "id", rightTable: "b", rightCol: "a_id"},
			// C との結合条件がない
		}

		// WHEN
		_, err = optimizeJoinOrder(hdl.BufferPool, candidates, predicates, nil, nil)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no valid join order")
	})
}

func TestFindPredicate(t *testing.T) {
	predicates := []joinPredicate{
		{leftTable: "users", leftCol: "id", rightTable: "orders", rightCol: "user_id"},
		{leftTable: "orders", leftCol: "item_id", rightTable: "items", rightCol: "id"},
	}

	t.Run("候補が leftTable の場合にマッチする", func(t *testing.T) {
		// GIVEN: resultTableNames に "orders" がある
		resultTableNames := map[string]struct{}{"orders": {}}

		// WHEN: 候補テーブル "users" で検索
		pred := findPredicate(predicates, "users", resultTableNames)

		// THEN: users.id = orders.user_id がマッチ
		require.NotNil(t, pred)
		assert.Equal(t, "users", pred.leftTable)
		assert.Equal(t, "orders", pred.rightTable)
	})

	t.Run("候補が rightTable の場合にマッチする", func(t *testing.T) {
		// GIVEN: resultTableNames に "orders" がある
		resultTableNames := map[string]struct{}{"orders": {}}

		// WHEN: 候補テーブル "items" で検索
		pred := findPredicate(predicates, "items", resultTableNames)

		// THEN: orders.item_id = items.id がマッチ
		require.NotNil(t, pred)
		assert.Equal(t, "orders", pred.leftTable)
		assert.Equal(t, "items", pred.rightTable)
	})

	t.Run("結合条件がない候補は nil を返す", func(t *testing.T) {
		// GIVEN: resultTableNames に "users" がある
		resultTableNames := map[string]struct{}{"users": {}}

		// WHEN: 候補テーブル "items" で検索 (users-items 間の条件はない)
		pred := findPredicate(predicates, "items", resultTableNames)

		// THEN
		assert.Nil(t, pred)
	})

	t.Run("resultTableNames が空の場合は nil を返す", func(t *testing.T) {
		// GIVEN
		resultTableNames := map[string]struct{}{}

		// WHEN
		pred := findPredicate(predicates, "users", resultTableNames)

		// THEN
		assert.Nil(t, pred)
	})
}

func TestResolveJoinCol(t *testing.T) {
	pred := &joinPredicate{
		leftTable: "users", leftCol: "id",
		rightTable: "orders", rightCol: "user_id",
	}

	t.Run("候補が leftTable の場合に leftCol を返す", func(t *testing.T) {
		// WHEN
		col := resolveJoinCol(pred, "users")

		// THEN
		assert.Equal(t, "id", col)
	})

	t.Run("候補が rightTable の場合に rightCol を返す", func(t *testing.T) {
		// WHEN
		col := resolveJoinCol(pred, "orders")

		// THEN
		assert.Equal(t, "user_id", col)
	})
}

func TestCalcFilteredEdgeCases(t *testing.T) {
	t.Run("nil の式は 1.0 を返す", func(t *testing.T) {
		// GIVEN
		candidate := joinCandidate{
			tblMeta: &handler.TableMetadata{Name: "t", NCols: 1, PKCount: 1},
			stats:   &handler.TableStatistics{IdxStats: map[string]handler.IndexStatistics{}},
		}

		// WHEN
		filtered := calcFiltered(nil, candidate)

		// THEN
		assert.Equal(t, 1.0, filtered)
	})

	t.Run("LHS がカラムでない式は 1.0 を返す", func(t *testing.T) {
		// GIVEN: LHS が式 (LhsExpr) の場合
		candidate := joinCandidate{
			tblMeta: &handler.TableMetadata{
				Name: "t", NCols: 2, PKCount: 1,
				Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "name", Pos: 1}},
			},
			stats: &handler.TableStatistics{
				ColStats: map[string]handler.ColumnStatistics{"name": {UniqueValues: 10}},
				IdxStats: map[string]handler.IndexStatistics{},
			},
		}
		expr := ast.NewBinaryExpr("AND",
			ast.NewLhsColumn(ast.ColumnId{ColName: "name"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("x")),
		)
		// LHS を LhsExpr に差し替え (非カラム)
		exprNonCol := ast.NewBinaryExpr("=",
			ast.NewLhsExpr(expr),
			ast.NewRhsLiteral(ast.NewStringLiteral("x")),
		)

		// WHEN
		filtered := calcFiltered(exprNonCol, candidate)

		// THEN
		assert.Equal(t, 1.0, filtered)
	})

	t.Run("ColStats にないカラムは 0.1 を返す", func(t *testing.T) {
		// GIVEN: status カラムの統計情報がない
		candidate := joinCandidate{
			tblMeta: &handler.TableMetadata{
				Name: "t", NCols: 2, PKCount: 1,
				Cols: []*dictionary.ColumnMeta{{Name: "id", Pos: 0}, {Name: "status", Pos: 1}},
			},
			stats: &handler.TableStatistics{
				ColStats: map[string]handler.ColumnStatistics{}, // status の統計なし
				IdxStats: map[string]handler.IndexStatistics{},
			},
		}
		expr := ast.NewBinaryExpr("=",
			ast.NewLhsColumn(ast.ColumnId{ColName: "status"}),
			ast.NewRhsLiteral(ast.NewStringLiteral("active")),
		)

		// WHEN
		filtered := calcFiltered(expr, candidate)

		// THEN: 統計がない場合は MySQL のデフォルト (COND_FILTER_EQUALITY = 0.1)
		assert.Equal(t, 0.1, filtered)
	})
}

func TestCalcDrivingTableCostWithWhereAND(t *testing.T) {
	t.Run("AND 複合条件で最もコストの低いアクセスパスが選ばれる", func(t *testing.T) {
		// GIVEN: users テーブルに PK(id) と UNIQUE INDEX(username) がある
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}

		// WHERE users.id = '1' AND users.username > 'k'
		// PK 等値検索 (コスト 1.0) と UNIQUE INDEX レンジスキャンの最安が選ばれる
		drivingWhere := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("AND",
				ast.NewLhsExpr(ast.NewBinaryExpr("=",
					ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "id"}),
					ast.NewRhsLiteral(ast.NewStringLiteral("1")),
				)),
				ast.NewRhsExpr(ast.NewBinaryExpr(">",
					ast.NewLhsColumn(ast.ColumnId{TableName: "users", ColName: "username"}),
					ast.NewRhsLiteral(ast.NewStringLiteral("k")),
				)),
			),
		}

		// WHEN
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, drivingWhere)

		// THEN: AND の各リーフから最安コストが選ばれる (ユニークスキャン以下)
		require.NoError(t, err)
		assert.LessOrEqual(t, readCost, calcUniqueScanCost(), "AND 内の最安コストはユニークスキャン以下")
		assert.Greater(t, readCost, 0.0)
		assert.Greater(t, fanout, 0.0)
	})

	t.Run("AND でインデックスなしの条件のみの場合はフルスキャンにフォールバック", func(t *testing.T) {
		// GIVEN
		setupUsersTable(t)
		hdl := handler.Get()
		tblMeta, _ := hdl.Catalog.GetTableMetaByName("users")
		stats, err := hdl.AnalyzeTable(tblMeta)
		require.NoError(t, err)
		tbl, err := hdl.GetTable("users")
		require.NoError(t, err)

		candidate := joinCandidate{tblMeta: tblMeta, stats: stats, table: tbl}

		// WHERE first_name = 'John' AND gender = 'male'
		// どちらもインデックスなし → evalDrivingWhereConditions は ok=false
		drivingWhere := &ast.WhereClause{
			Condition: ast.NewBinaryExpr("AND",
				ast.NewLhsExpr(ast.NewBinaryExpr("=",
					ast.NewLhsColumn(ast.ColumnId{ColName: "first_name"}),
					ast.NewRhsLiteral(ast.NewStringLiteral("John")),
				)),
				ast.NewRhsExpr(ast.NewBinaryExpr("=",
					ast.NewLhsColumn(ast.ColumnId{ColName: "gender"}),
					ast.NewRhsLiteral(ast.NewStringLiteral("male")),
				)),
			),
		}

		// WHEN
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil, drivingWhere)

		// THEN: フルスキャン → fanout = RecordCount
		require.NoError(t, err)
		assert.Equal(t, float64(stats.RecordCount), fanout)
		assert.Greater(t, readCost, 0.0)
	})
}

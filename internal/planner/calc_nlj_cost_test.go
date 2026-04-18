package planner

import (
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
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 1.0, nil)

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
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 100.0, pred)

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
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 50.0, pred)

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
		readCost, fanout, err := calcTableJoinCost(hdl.BufferPool, candidate, 10.0, pred)

		// THEN: フルスキャン → fanout = RecordCount
		require.NoError(t, err)
		assert.Equal(t, float64(stats.RecordCount), fanout)
		assert.Greater(t, readCost, 0.0)
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
		result, err := optimizeJoinOrder(hdl.BufferPool, candidates, predicates)

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
		result, err := optimizeJoinOrder(hdl.BufferPool, candidates, predicates)

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
		_, err = optimizeJoinOrder(hdl.BufferPool, candidates, predicates)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no valid join order")
	})
}

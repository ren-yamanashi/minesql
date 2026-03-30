package statistics

import (
	"minesql/internal/engine"
	"minesql/internal/executor"
	"minesql/internal/storage/access"
	"minesql/internal/storage/catalog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalyze(t *testing.T) {
	t.Run("レコード数が正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

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
		stats := NewStatistics(meta, eng.BufferPool)

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
		stats := NewStatistics(meta, eng.BufferPool)

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
		stats := NewStatistics(meta, eng.BufferPool)

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

	t.Run("プライマリキー B+Tree の高さが正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコードは 1 ページに収まるので高さ 1
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: H(T) = 1 (ルートリーフのみ)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), result.PrimaryHeight)
	})

	t.Run("セカンダリインデックスの高さが正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコードでは B+Tree の高さは 1 (ルートリーフのみ)
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: ユニークインデックスが 1 つ、高さ 1
		assert.NoError(t, err)
		assert.Len(t, result.SecondaryIndexStats, 1)
		for _, idxStat := range result.SecondaryIndexStats {
			assert.Equal(t, uint64(1), idxStat.Height)
		}
	})

	t.Run("セカンダリインデックスのリーフページ数が正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコードは 1 リーフページに収まる
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: Bl(I) = 1
		assert.NoError(t, err)
		assert.Len(t, result.SecondaryIndexStats, 1)
		for _, idxStat := range result.SecondaryIndexStats {
			assert.Equal(t, uint64(1), idxStat.LeafPageCount)
		}
	})

	t.Run("INSERT 後に再 Analyze するとレコード数やユニーク値数が増加する", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		before, err := stats.Analyze()
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), before.RecordCount)
		assert.Equal(t, uint64(2), before.ColumnStats["category"].UniqueValues)

		// WHEN: 新しいカテゴリを持つレコードを追加
		insertRecords(t, "products",
			[]executor.Record{
				{[]byte("4"), []byte("Donut"), []byte("Snack")},
			},
		)

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
		stats := NewStatistics(meta, eng.BufferPool)

		before, err := stats.Analyze()
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), before.RecordCount)
		assert.Equal(t, uint64(3), before.ColumnStats["name"].UniqueValues)

		// WHEN: "Carrot" (唯一の "Veggie") を削除
		deleteByCondition(t, "products", func(record executor.Record) bool {
			return string(record[0]) == "3" // id = "3"
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

	t.Run("DELETE で最小値のレコードを削除すると min が更新される", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み (id: "1", "2", "3")
		setupStatisticsTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("products")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		before, err := stats.Analyze()
		assert.NoError(t, err)
		assert.Equal(t, []byte("1"), before.ColumnStats["id"].MinValue)

		// WHEN: id = "1" (最小値) のレコードを削除
		deleteByCondition(t, "products", func(record executor.Record) bool {
			return string(record[0]) == "1"
		})

		after, err := stats.Analyze()

		// THEN: min(id) が "1" -> "2" に変化
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), after.RecordCount)
		assert.Equal(t, []byte("2"), after.ColumnStats["id"].MinValue)
		assert.Equal(t, []byte("3"), after.ColumnStats["id"].MaxValue)
	})

	t.Run("全レコードの値が同一のカラムではユニーク値数が 1 になる", func(t *testing.T) {
		// GIVEN: category がすべて "Fruit" のレコード
		setupSameValueTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("same_values")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: V(T, category) = 1, min = max = "Fruit"
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.RecordCount)
		assert.Equal(t, uint64(1), result.ColumnStats["category"].UniqueValues)
		assert.Equal(t, []byte("Fruit"), result.ColumnStats["category"].MinValue)
		assert.Equal(t, []byte("Fruit"), result.ColumnStats["category"].MaxValue)
	})

	t.Run("レコードが 1 件のみの場合の統計値が正しい", func(t *testing.T) {
		// GIVEN: 1 レコードのみのテーブル
		setupSingleRecordTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("single")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: R(T) = 1, V(T, F) = 1, min = max
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), result.RecordCount)
		assert.Equal(t, uint64(1), result.ColumnStats["id"].UniqueValues)
		assert.Equal(t, uint64(1), result.ColumnStats["name"].UniqueValues)
		assert.Equal(t, []byte("1"), result.ColumnStats["id"].MinValue)
		assert.Equal(t, []byte("1"), result.ColumnStats["id"].MaxValue)
		assert.Equal(t, []byte("Alice"), result.ColumnStats["name"].MinValue)
		assert.Equal(t, []byte("Alice"), result.ColumnStats["name"].MaxValue)
	})

	t.Run("複数のセカンダリインデックスがそれぞれ統計を持つ", func(t *testing.T) {
		// GIVEN: 2 つのセカンダリインデックスを持つテーブル
		setupMultiIndexTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("multi_idx")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: セカンダリインデックスが 2 つ
		assert.NoError(t, err)
		assert.Len(t, result.SecondaryIndexStats, 2)

		for idxName, idxStat := range result.SecondaryIndexStats {
			assert.Equal(t, uint64(1), idxStat.Height, "index %s: H(I) should be 1", idxName)
			assert.Equal(t, uint64(1), idxStat.LeafPageCount, "index %s: Bl(I) should be 1", idxName)
		}
	})

	t.Run("空テーブルではレコード数 0 でカラム統計も空になる", func(t *testing.T) {
		// GIVEN: テーブルを作成するがデータは挿入しない
		setupEmptyTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("items")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

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

	t.Run("空テーブルのプライマリキー高さが 1 になる", func(t *testing.T) {
		// GIVEN: データなしの空テーブル
		setupEmptyTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("items")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: 空でもルートリーフは存在するので H(T) = 1
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), result.PrimaryHeight)
	})

	t.Run("空テーブルのカラム min/max は nil になる", func(t *testing.T) {
		// GIVEN: データなしの空テーブル
		setupEmptyTable(t)
		defer engine.Reset()

		eng := engine.Get()
		meta, ok := eng.Catalog.GetTableMetadataByName("items")
		assert.True(t, ok)
		stats := NewStatistics(meta, eng.BufferPool)

		// WHEN
		result, err := stats.Analyze()

		// THEN: レコードがないので min/max は nil
		assert.NoError(t, err)
		for _, colStat := range result.ColumnStats {
			assert.Nil(t, colStat.MinValue)
			assert.Nil(t, colStat.MaxValue)
		}
	})
}

// createTable はテスト用にテーブルを作成する
func createTable(t *testing.T, tableName string, primaryKeyCount uint8, indexes []*executor.IndexParam, columns []*executor.ColumnParam) { //nolint:unparam
	t.Helper()
	ct := executor.NewCreateTable(tableName, primaryKeyCount, indexes, columns)
	_, err := ct.Next()
	assert.NoError(t, err)
}

// getTable はテスト用にテーブルのアクセスメソッドを取得する
func getTable(t *testing.T, tableName string) *access.TableAccessMethod {
	t.Helper()
	eng := engine.Get()
	meta, ok := eng.Catalog.GetTableMetadataByName(tableName)
	assert.True(t, ok)
	tbl, err := meta.GetTable()
	assert.NoError(t, err)
	return tbl
}

// insertRecords はテスト用にレコードを挿入する
func insertRecords(t *testing.T, tableName string, records []executor.Record) { //nolint:unparam
	t.Helper()
	var trxId engine.TrxId = 1
	tbl := getTable(t, tableName)
	ins := executor.NewInsert(trxId, tbl, records)
	_, err := ins.Next()
	assert.NoError(t, err)
}

// deleteByCondition はテスト用に条件に合致するレコードを削除する
func deleteByCondition(t *testing.T, tableName string, cond func(executor.Record) bool) {
	t.Helper()
	var trxId engine.TrxId = 1
	tbl := getTable(t, tableName)
	del := executor.NewDelete(trxId, tbl, executor.NewFilter(
		executor.NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record executor.Record) bool { return true },
		),
		cond,
	))
	_, err := del.Next()
	assert.NoError(t, err)
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

	createTable(t, "products", 1,
		[]*executor.IndexParam{
			{Name: "idx_name", ColName: "name", SecondaryKey: 1},
		},
		[]*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
			{Name: "category", Type: catalog.ColumnTypeString},
		},
	)

	insertRecords(t, "products",
		[]executor.Record{
			{[]byte("1"), []byte("Apple"), []byte("Fruit")},
			{[]byte("2"), []byte("Banana"), []byte("Fruit")},
			{[]byte("3"), []byte("Carrot"), []byte("Veggie")},
		},
	)
}

// setupEmptyTable はストレージを初期化し、データなしの空テーブルを作成する
func setupEmptyTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	createTable(t, "items", 1,
		nil,
		[]*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		},
	)
}

// setupSameValueTable はストレージを初期化し、全レコードの category が同一のテーブルを作成する
//
// テーブル: same_values (id, category)
//
//	| id  | category |
//	| --- | -------- |
//	| 1   | Fruit    |
//	| 2   | Fruit    |
//	| 3   | Fruit    |
func setupSameValueTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	createTable(t, "same_values", 1,
		nil,
		[]*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "category", Type: catalog.ColumnTypeString},
		},
	)

	insertRecords(t, "same_values",
		[]executor.Record{
			{[]byte("1"), []byte("Fruit")},
			{[]byte("2"), []byte("Fruit")},
			{[]byte("3"), []byte("Fruit")},
		},
	)
}

// setupSingleRecordTable はストレージを初期化し、1 レコードのみのテーブルを作成する
//
// テーブル: single (id, name)
//
//	| id  | name  |
//	| --- | ----- |
//	| 1   | Alice |
func setupSingleRecordTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	createTable(t, "single", 1,
		nil,
		[]*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
		},
	)

	insertRecords(t, "single",
		[]executor.Record{
			{[]byte("1"), []byte("Alice")},
		},
	)
}

// setupMultiIndexTable はストレージを初期化し、2 つのセカンダリインデックスを持つテーブルを作成する
//
// テーブル: multi_idx (id, name, email)
//
//   - プライマリキー: id
//
//   - ユニークインデックス: name, email
//
//     | id  | name   | email         |
//     | --- | ------ | ------------- |
//     | 1   | Alice  | alice@test    |
//     | 2   | Bob    | bob@test      |
func setupMultiIndexTable(t *testing.T) {
	t.Helper()

	tmpdir := t.TempDir()
	t.Setenv("MINESQL_DATA_DIR", tmpdir)
	t.Setenv("MINESQL_BUFFER_SIZE", "100")
	engine.Reset()
	engine.Init()

	createTable(t, "multi_idx", 1,
		[]*executor.IndexParam{
			{Name: "idx_name", ColName: "name", SecondaryKey: 1},
			{Name: "idx_email", ColName: "email", SecondaryKey: 2},
		},
		[]*executor.ColumnParam{
			{Name: "id", Type: catalog.ColumnTypeString},
			{Name: "name", Type: catalog.ColumnTypeString},
			{Name: "email", Type: catalog.ColumnTypeString},
		},
	)

	insertRecords(t, "multi_idx",
		[]executor.Record{
			{[]byte("1"), []byte("Alice"), []byte("alice@test")},
			{[]byte("2"), []byte("Bob"), []byte("bob@test")},
		},
	)
}

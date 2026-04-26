package dictionary

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/access"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestAnalyze(t *testing.T) {
	t.Run("レコード数が正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: R(T) = 3
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.RecordCount)
	})

	t.Run("リーフページ数が正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコードは 1 ページに収まる
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: P(T) = 1
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), result.LeafPageCount)
	})

	t.Run("各カラムのユニーク値数が正しく算出される", func(t *testing.T) {
		// GIVEN: id は全件異なる、name は全件異なる、category は "Fruit" が重複
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.ColStats["id"].UniqueValues)       // V(T, id) = 3
		assert.Equal(t, uint64(3), result.ColStats["name"].UniqueValues)     // V(T, name) = 3
		assert.Equal(t, uint64(2), result.ColStats["category"].UniqueValues) // V(T, category) = 2
	})

	t.Run("各カラムの min/max が正しく算出される", func(t *testing.T) {
		// GIVEN
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN
		assert.NoError(t, err)

		assert.Equal(t, []byte("1"), result.ColStats["id"].MinValue)
		assert.Equal(t, []byte("3"), result.ColStats["id"].MaxValue)

		assert.Equal(t, []byte("Apple"), result.ColStats["name"].MinValue)
		assert.Equal(t, []byte("Carrot"), result.ColStats["name"].MaxValue)

		assert.Equal(t, []byte("Fruit"), result.ColStats["category"].MinValue)
		assert.Equal(t, []byte("Veggie"), result.ColStats["category"].MaxValue)
	})

	t.Run("プライマリキー B+Tree の高さが正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコードは 1 ページに収まるので高さ 1
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: H(T) = 1 (ルートリーフのみ)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), result.TreeHeight)
	})

	t.Run("セカンダリインデックスの高さが正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコードでは B+Tree の高さは 1 (ルートリーフのみ)
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: ユニークインデックスが 1 つ、高さ 1、RecPerKey 1.0
		assert.NoError(t, err)
		assert.Len(t, result.IdxStats, 1)
		for _, idxStat := range result.IdxStats {
			assert.Equal(t, uint64(1), idxStat.Height)
			assert.Equal(t, 1.0, idxStat.RecPerKey)
		}
	})

	t.Run("セカンダリインデックスのリーフページ数が正しく算出される", func(t *testing.T) {
		// GIVEN: 3 レコードは 1 リーフページに収まる
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: Bl(I) = 1
		assert.NoError(t, err)
		assert.Len(t, result.IdxStats, 1)
		for _, idxStat := range result.IdxStats {
			assert.Equal(t, uint64(1), idxStat.LeafPageCount)
		}
	})

	t.Run("INSERT 後に再 Analyze するとレコード数やユニーク値数が増加する", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		before, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), before.RecordCount)
		assert.Equal(t, uint64(2), before.ColStats["category"].UniqueValues)

		// WHEN: 新しいカテゴリを持つレコードを追加
		tbl := env.tables["products"]
		err = tbl.Insert(env.bp, 0, lock.NewManager(5000), [][]byte{[]byte("4"), []byte("Donut"), []byte("Snack")})
		assert.NoError(t, err)

		after, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: R(T) が 3 -> 4 に増加
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), after.RecordCount)
		// V(T, category) が 2 -> 3 に増加 ("Snack" が追加)
		assert.Equal(t, uint64(3), after.ColStats["category"].UniqueValues)
		// V(T, name) が 3 -> 4 に増加
		assert.Equal(t, uint64(4), after.ColStats["name"].UniqueValues)
		// max(name) が "Carrot" -> "Donut" に変化
		assert.Equal(t, []byte("Donut"), after.ColStats["name"].MaxValue)
	})

	t.Run("DELETE 後に再 Analyze するとレコード数やユニーク値数が減少する", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		before, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), before.RecordCount)
		assert.Equal(t, uint64(3), before.ColStats["name"].UniqueValues)

		// WHEN: "Carrot" (唯一の "Veggie") を削除
		deleteByCondition(t, env, "products", func(record [][]byte) bool {
			return string(record[0]) == "3" // id = "3"
		})

		after, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: R(T) が 3 -> 2 に減少
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), after.RecordCount)
		// V(T, name) が 3 -> 2 に減少
		assert.Equal(t, uint64(2), after.ColStats["name"].UniqueValues)
		// V(T, category) が 2 -> 1 に減少 ("Veggie" がなくなる)
		assert.Equal(t, uint64(1), after.ColStats["category"].UniqueValues)
		// max(name) が "Carrot" -> "Banana" に変化
		assert.Equal(t, []byte("Banana"), after.ColStats["name"].MaxValue)
	})

	t.Run("DELETE で最小値のレコードを削除すると min が更新される", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み (id: "1", "2", "3")
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		before, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)
		assert.NoError(t, err)
		assert.Equal(t, []byte("1"), before.ColStats["id"].MinValue)

		// WHEN: id = "1" (最小値) のレコードを削除
		deleteByCondition(t, env, "products", func(record [][]byte) bool {
			return string(record[0]) == "1"
		})

		after, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: min(id) が "1" -> "2" に変化
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), after.RecordCount)
		assert.Equal(t, []byte("2"), after.ColStats["id"].MinValue)
		assert.Equal(t, []byte("3"), after.ColStats["id"].MaxValue)
	})

	t.Run("全レコードの値が同一のカラムではユニーク値数が 1 になる", func(t *testing.T) {
		// GIVEN: category がすべて "Fruit" のレコード
		env := setupSameValueTable(t)

		meta, ok := env.catalog.GetTableMetaByName("same_values")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: V(T, category) = 1, min = max = "Fruit"
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.RecordCount)
		assert.Equal(t, uint64(1), result.ColStats["category"].UniqueValues)
		assert.Equal(t, []byte("Fruit"), result.ColStats["category"].MinValue)
		assert.Equal(t, []byte("Fruit"), result.ColStats["category"].MaxValue)
	})

	t.Run("レコードが 1 件のみの場合の統計値が正しい", func(t *testing.T) {
		// GIVEN: 1 レコードのみのテーブル
		env := setupSingleRecordTable(t)

		meta, ok := env.catalog.GetTableMetaByName("single")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: R(T) = 1, V(T, F) = 1, min = max
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), result.RecordCount)
		assert.Equal(t, uint64(1), result.ColStats["id"].UniqueValues)
		assert.Equal(t, uint64(1), result.ColStats["name"].UniqueValues)
		assert.Equal(t, []byte("1"), result.ColStats["id"].MinValue)
		assert.Equal(t, []byte("1"), result.ColStats["id"].MaxValue)
		assert.Equal(t, []byte("Alice"), result.ColStats["name"].MinValue)
		assert.Equal(t, []byte("Alice"), result.ColStats["name"].MaxValue)
	})

	t.Run("複数のセカンダリインデックスがそれぞれ統計を持つ", func(t *testing.T) {
		// GIVEN: 2 つのセカンダリインデックスを持つテーブル
		env := setupMultiIndexTable(t)

		meta, ok := env.catalog.GetTableMetaByName("multi_idx")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: セカンダリインデックスが 2 つ
		assert.NoError(t, err)
		assert.Len(t, result.IdxStats, 2)

		for idxName, idxStat := range result.IdxStats {
			assert.Equal(t, uint64(1), idxStat.Height, "index %s: H(I) should be 1", idxName)
			assert.Equal(t, uint64(1), idxStat.LeafPageCount, "index %s: Bl(I) should be 1", idxName)
		}
	})

	t.Run("空テーブルではレコード数 0 でカラム統計も空になる", func(t *testing.T) {
		// GIVEN: テーブルを作成するがデータは挿入しない
		env := setupEmptyTable(t)

		meta, ok := env.catalog.GetTableMetaByName("items")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), result.RecordCount)
		assert.Equal(t, uint64(1), result.LeafPageCount)
		assert.Empty(t, result.IdxStats)

		// カラム統計は存在するがユニーク値数は 0
		for _, colStat := range result.ColStats {
			assert.Equal(t, uint64(0), colStat.UniqueValues)
		}
	})

	t.Run("空テーブルのプライマリキー高さが 1 になる", func(t *testing.T) {
		// GIVEN: データなしの空テーブル
		env := setupEmptyTable(t)

		meta, ok := env.catalog.GetTableMetaByName("items")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: 空でもルートリーフは存在するので H(T) = 1
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), result.TreeHeight)
	})

	t.Run("空テーブルのカラム min/max は nil になる", func(t *testing.T) {
		// GIVEN: データなしの空テーブル
		env := setupEmptyTable(t)

		meta, ok := env.catalog.GetTableMetaByName("items")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: レコードがないので min/max は nil
		assert.NoError(t, err)
		for _, colStat := range result.ColStats {
			assert.Nil(t, colStat.MinValue)
			assert.Nil(t, colStat.MaxValue)
		}
	})

	t.Run("非ユニークインデックスの RecPerKey が重複数に応じて算出される", func(t *testing.T) {
		// GIVEN: category カラムに非ユニークインデックス。"Fruit" が 2 件、"Veggie" が 1 件
		env := setupNonUniqueIndexTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products_nu")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: RecPerKey = 3 (レコード数) / 2 (distinct キー数) = 1.5
		assert.NoError(t, err)
		assert.Len(t, result.IdxStats, 1)
		assert.Equal(t, 1.5, result.IdxStats["idx_category"].RecPerKey)
	})

	t.Run("非ユニークインデックスで全レコードが同一キーの場合 RecPerKey がレコード数と一致する", func(t *testing.T) {
		// GIVEN: category がすべて "Fruit" (distinct = 1)
		env := setupTestEnv(t)
		createTable(t, env, "same_cat", 1,
			[]indexParam{
				{name: "idx_category", colName: "category", secondaryKey: 2, unique: false},
			},
			[]columnParam{
				{name: "id", columnType: ColumnTypeString},
				{name: "name", columnType: ColumnTypeString},
				{name: "category", columnType: ColumnTypeString},
			},
		)
		insertRecords(t, env, "same_cat", [][][]byte{
			{[]byte("1"), []byte("Apple"), []byte("Fruit")},
			{[]byte("2"), []byte("Banana"), []byte("Fruit")},
			{[]byte("3"), []byte("Cherry"), []byte("Fruit")},
		})

		meta, ok := env.catalog.GetTableMetaByName("same_cat")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: RecPerKey = 3 / 1 = 3.0
		assert.NoError(t, err)
		assert.Equal(t, 3.0, result.IdxStats["idx_category"].RecPerKey)
	})

	t.Run("ユニークインデックスの RecPerKey は常に 1.0", func(t *testing.T) {
		// GIVEN: name に UNIQUE INDEX
		env := setupStatsTable(t)

		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: UNIQUE なので RecPerKey = 1.0
		assert.NoError(t, err)
		assert.Equal(t, 1.0, result.IdxStats["idx_name"].RecPerKey)
	})

	t.Run("非ユニークインデックスでソフトデリート済みレコードは RecPerKey の算出から除外される", func(t *testing.T) {
		// GIVEN: 3 レコード挿入後、1 件をソフトデリート
		env := setupNonUniqueIndexTable(t)
		tbl := env.tables["products_nu"]

		// "Banana" (category="Fruit") をソフトデリート → active は "Apple"(Fruit) と "Carrot"(Veggie)
		err := tbl.SoftDelete(env.bp, 0, lock.NewManager(5000), [][]byte{[]byte("2"), []byte("Banana"), []byte("Fruit")})
		assert.NoError(t, err)

		meta, ok := env.catalog.GetTableMetaByName("products_nu")
		assert.True(t, ok)

		// WHEN
		result, err := (&StatsCollector{bufferPool: env.bp, states: make(map[string]*tableState)}).Analyze(meta)

		// THEN: active 2 件 / distinct 2 キー = 1.0
		assert.NoError(t, err)
		assert.Equal(t, 1.0, result.IdxStats["idx_category"].RecPerKey)
	})
}

func TestBuildTable(t *testing.T) {
	t.Run("インデックスなしのテーブルを構築できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
		}
		tableMeta := NewTableMeta(1, "users", 2, 1, colMeta, []*IndexMeta{}, page.NewPageId(page.FileId(1), 0))

		// WHEN
		tbl, err := buildTable(&tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, uint8(1), tbl.PrimaryKeyCount)
		assert.Equal(t, 0, len(tbl.SecondaryIndexes))
		assert.Equal(t, page.NewPageId(page.FileId(1), 0), tbl.MetaPageId)
	})

	t.Run("ユニークインデックス付きのテーブルを構築できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "email", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMeta(1, "users", 2, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		tbl, err := buildTable(&tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, "users", tbl.Name)
		assert.Equal(t, 1, len(tbl.SecondaryIndexes))
		assert.Equal(t, "idx_email", tbl.SecondaryIndexes[0].Name)
		assert.Equal(t, "email", tbl.SecondaryIndexes[0].ColName)
		assert.Equal(t, uint16(1), tbl.SecondaryIndexes[0].ColIdx)
		assert.Equal(t, uint8(1), tbl.SecondaryIndexes[0].PkCount)
		assert.Equal(t, page.NewPageId(page.FileId(1), 1), tbl.SecondaryIndexes[0].MetaPageId)
	})

	t.Run("複数のユニークインデックス付きのテーブルを構築できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "email", 1, ColumnTypeString),
			NewColumnMeta(1, "username", 2, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
			NewIndexMeta(1, "idx_username", "username", IndexTypeUnique, page.NewPageId(page.FileId(1), 2)),
		}
		tableMeta := NewTableMeta(1, "users", 3, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		tbl, err := buildTable(&tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, 2, len(tbl.SecondaryIndexes))
		assert.Equal(t, "idx_email", tbl.SecondaryIndexes[0].Name)
		assert.Equal(t, "idx_username", tbl.SecondaryIndexes[1].Name)
	})

	t.Run("存在しないカラム名を指定したインデックスがある場合、エラーを返す", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "name", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMeta(1, "users", 2, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		tbl, err := buildTable(&tableMeta)

		// THEN
		assert.Error(t, err)
		assert.Nil(t, tbl)
		assert.Contains(t, err.Error(), "column email not found in table users")
	})

	t.Run("非ユニークインデックス付きのテーブルを構築できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "category", 1, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_category", "category", IndexTypeNonUnique, page.NewPageId(page.FileId(1), 1)),
		}
		tableMeta := NewTableMeta(1, "items", 2, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		tbl, err := buildTable(&tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, 1, len(tbl.SecondaryIndexes))
		assert.Equal(t, "idx_category", tbl.SecondaryIndexes[0].Name)
		assert.False(t, tbl.SecondaryIndexes[0].Unique)
	})

	t.Run("ユニークと非ユニークのインデックスが混在するテーブルを構築できる", func(t *testing.T) {
		// GIVEN
		colMeta := []*ColumnMeta{
			NewColumnMeta(1, "id", 0, ColumnTypeString),
			NewColumnMeta(1, "email", 1, ColumnTypeString),
			NewColumnMeta(1, "category", 2, ColumnTypeString),
		}
		idxMeta := []*IndexMeta{
			NewIndexMeta(1, "idx_email", "email", IndexTypeUnique, page.NewPageId(page.FileId(1), 1)),
			NewIndexMeta(1, "idx_category", "category", IndexTypeNonUnique, page.NewPageId(page.FileId(1), 2)),
		}
		tableMeta := NewTableMeta(1, "items", 3, 1, colMeta, idxMeta, page.NewPageId(page.FileId(1), 0))

		// WHEN
		tbl, err := buildTable(&tableMeta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(tbl.SecondaryIndexes))
		assert.True(t, tbl.SecondaryIndexes[0].Unique)
		assert.False(t, tbl.SecondaryIndexes[1].Unique)
	})
}

func TestGetOrAnalyze(t *testing.T) {
	t.Run("初回呼び出しで Analyze が実行されキャッシュされる", func(t *testing.T) {
		// GIVEN: 3 レコード挿入済み
		env := setupStatsTable(t)
		sc := NewStatsCollector(env.bp)
		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)

		// WHEN
		result, err := sc.GetOrAnalyze(meta)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.RecordCount)
	})

	t.Run("dirty_count が閾値以下ならキャッシュが返る", func(t *testing.T) {
		// GIVEN: 3 レコードで GetOrAnalyze 済み
		env := setupStatsTable(t)
		sc := NewStatsCollector(env.bp)
		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)
		_, err := sc.GetOrAnalyze(meta)
		assert.NoError(t, err)

		// WHEN: dirty_count=0 のまま GetOrAnalyze を呼ぶ
		result, err := sc.GetOrAnalyze(meta)

		// THEN: キャッシュされた統計値が返る
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), result.RecordCount)
	})

	t.Run("dirty_count が閾値を超えると再 Analyze が実行される", func(t *testing.T) {
		// GIVEN: 3 レコードで GetOrAnalyze 済み
		env := setupStatsTable(t)
		sc := NewStatsCollector(env.bp)
		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)
		_, err := sc.GetOrAnalyze(meta)
		assert.NoError(t, err)

		// WHEN: 1 レコード追加して dirty_count を加算
		tbl := env.tables["products"]
		err = tbl.Insert(env.bp, 0, lock.NewManager(5000), [][]byte{[]byte("4"), []byte("Donut"), []byte("Snack")})
		assert.NoError(t, err)
		sc.IncrementDirtyCount("products", 1)
		result, err := sc.GetOrAnalyze(meta)

		// THEN: 再 Analyze が実行され、最新の統計値が返る
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), result.RecordCount)
	})

	t.Run("再 Analyze 後に dirty_count がリセットされる", func(t *testing.T) {
		// GIVEN: dirty_count 加算 → 再 Analyze 済み
		env := setupStatsTable(t)
		sc := NewStatsCollector(env.bp)
		meta, ok := env.catalog.GetTableMetaByName("products")
		assert.True(t, ok)
		_, err := sc.GetOrAnalyze(meta)
		assert.NoError(t, err)
		tbl := env.tables["products"]
		err = tbl.Insert(env.bp, 0, lock.NewManager(5000), [][]byte{[]byte("4"), []byte("Donut"), []byte("Snack")})
		assert.NoError(t, err)
		sc.IncrementDirtyCount("products", 1)
		_, err = sc.GetOrAnalyze(meta)
		assert.NoError(t, err)

		// WHEN: さらに 1 レコード追加するが dirty_count は加算しない
		err = tbl.Insert(env.bp, 0, lock.NewManager(5000), [][]byte{[]byte("5"), []byte("Egg"), []byte("Dairy")})
		assert.NoError(t, err)
		result, err := sc.GetOrAnalyze(meta)

		// THEN: キャッシュが返る (RecordCount は再 Analyze 時の 4 のまま)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), result.RecordCount)
	})
}

// testEnv はテスト用の環境を保持する
type testEnv struct {
	bp      *buffer.BufferPool
	catalog *Catalog
	tables  map[string]*access.Table
}

// setupTestEnv はテスト用に BufferPool とカタログを初期化する
func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()
	tmpdir := t.TempDir()

	bp := buffer.NewBufferPool(100, nil)

	// カタログ用の Disk を登録
	catalogFileId := page.FileId(0)
	catalogDm, err := file.NewDisk(catalogFileId, filepath.Join(tmpdir, "minesql.db"))
	assert.NoError(t, err)
	bp.RegisterDisk(catalogFileId, catalogDm)

	cat, err := CreateCatalog(bp)
	assert.NoError(t, err)

	return &testEnv{
		bp:      bp,
		catalog: cat,
		tables:  make(map[string]*access.Table),
	}
}

// indexParam はテスト用のインデックスパラメータ
type indexParam struct {
	name         string
	colName      string
	secondaryKey uint16
	unique       bool
}

// columnParam はテスト用のカラムパラメータ
type columnParam struct {
	name       string
	columnType ColumnType
}

// createTable はテスト用にテーブルを作成し、カタログに登録する
func createTable(t *testing.T, env *testEnv, tableName string, pkCount uint8, indexes []indexParam, columns []columnParam) { //nolint:unparam
	t.Helper()

	// FileId を採番
	fileId, err := env.catalog.AllocateFileId(env.bp)
	assert.NoError(t, err)

	// テーブル用の Disk を登録
	tmpdir := t.TempDir()
	dm, err := file.NewDisk(fileId, filepath.Join(tmpdir, tableName+".db"))
	assert.NoError(t, err)
	env.bp.RegisterDisk(fileId, dm)

	// テーブルの metaPageId を設定
	metaPageId, err := env.bp.AllocatePageId(fileId)
	assert.NoError(t, err)

	// 各セカンダリインデックスの metaPageId を設定
	secondaryIndexes := make([]*access.SecondaryIndex, len(indexes))
	for i, idx := range indexes {
		indexMetaPageId, err := env.bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		si := access.NewSecondaryIndex(idx.name, idx.colName, indexMetaPageId, idx.secondaryKey, pkCount, idx.unique)
		err = si.Create(env.bp)
		assert.NoError(t, err)
		secondaryIndexes[i] = si
	}

	// テーブルを作成
	tbl := access.NewTable(tableName, metaPageId, pkCount, secondaryIndexes, nil, nil)
	err = tbl.Create(env.bp)
	assert.NoError(t, err)

	// インデックスのメタデータを作成
	idxMeta := make([]*IndexMeta, len(indexes))
	for i, si := range secondaryIndexes {
		idxType := IndexTypeNonUnique
		if indexes[i].unique {
			idxType = IndexTypeUnique
		}
		idxMeta[i] = NewIndexMeta(fileId, si.Name, si.ColName, idxType, si.MetaPageId)
	}

	// カラムのメタデータを作成
	colMeta := make([]*ColumnMeta, len(columns))
	for i, col := range columns {
		colMeta[i] = NewColumnMeta(fileId, col.name, uint16(i), col.columnType)
	}

	// テーブルメタデータを作成してカタログに登録
	tblMeta := NewTableMeta(fileId, tableName, uint8(len(columns)), pkCount, colMeta, idxMeta, metaPageId)
	err = env.catalog.Insert(env.bp, tblMeta)
	assert.NoError(t, err)

	env.tables[tableName] = &tbl
}

// insertRecords はテスト用にレコードを挿入する
func insertRecords(t *testing.T, env *testEnv, tableName string, records [][][]byte) {
	t.Helper()
	tbl := env.tables[tableName]
	for _, record := range records {
		err := tbl.Insert(env.bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)
	}
}

// deleteByCondition はテスト用に条件に合致するレコードをソフトデリートする
func deleteByCondition(t *testing.T, env *testEnv, tableName string, cond func([][]byte) bool) {
	t.Helper()
	tbl := env.tables[tableName]

	// 削除対象のレコードを先にすべて取得する
	iter, err := tbl.Search(env.bp, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
	assert.NoError(t, err)

	var targets [][][]byte
	for {
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		if !ok {
			break
		}
		if cond(record) {
			targets = append(targets, record)
		}
	}

	// 取得したレコードを削除
	for _, record := range targets {
		err := tbl.SoftDelete(env.bp, 0, lock.NewManager(5000), record)
		assert.NoError(t, err)
	}
}

// setupStatsTable はストレージを初期化し、統計情報テスト用のテーブルを作成する
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
func setupStatsTable(t *testing.T) *testEnv {
	t.Helper()

	env := setupTestEnv(t)

	createTable(t, env, "products", 1,
		[]indexParam{
			{name: "idx_name", colName: "name", secondaryKey: 1, unique: true},
		},
		[]columnParam{
			{name: "id", columnType: ColumnTypeString},
			{name: "name", columnType: ColumnTypeString},
			{name: "category", columnType: ColumnTypeString},
		},
	)

	insertRecords(t, env, "products",
		[][][]byte{
			{[]byte("1"), []byte("Apple"), []byte("Fruit")},
			{[]byte("2"), []byte("Banana"), []byte("Fruit")},
			{[]byte("3"), []byte("Carrot"), []byte("Veggie")},
		},
	)

	return env
}

// setupNonUniqueIndexTable はストレージを初期化し、非ユニークインデックス付きのテーブルを作成する
//
// テーブル: products_nu (id PK, name, category)
// インデックス: idx_category (category, 非ユニーク)
// データ: ("1","Apple","Fruit"), ("2","Banana","Fruit"), ("3","Carrot","Veggie")
func setupNonUniqueIndexTable(t *testing.T) *testEnv {
	t.Helper()

	env := setupTestEnv(t)

	createTable(t, env, "products_nu", 1,
		[]indexParam{
			{name: "idx_category", colName: "category", secondaryKey: 2, unique: false},
		},
		[]columnParam{
			{name: "id", columnType: ColumnTypeString},
			{name: "name", columnType: ColumnTypeString},
			{name: "category", columnType: ColumnTypeString},
		},
	)

	insertRecords(t, env, "products_nu",
		[][][]byte{
			{[]byte("1"), []byte("Apple"), []byte("Fruit")},
			{[]byte("2"), []byte("Banana"), []byte("Fruit")},
			{[]byte("3"), []byte("Carrot"), []byte("Veggie")},
		},
	)

	return env
}

// setupEmptyTable はストレージを初期化し、データなしの空テーブルを作成する
func setupEmptyTable(t *testing.T) *testEnv {
	t.Helper()

	env := setupTestEnv(t)

	createTable(t, env, "items", 1,
		nil,
		[]columnParam{
			{name: "id", columnType: ColumnTypeString},
			{name: "name", columnType: ColumnTypeString},
		},
	)

	return env
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
func setupSameValueTable(t *testing.T) *testEnv {
	t.Helper()

	env := setupTestEnv(t)

	createTable(t, env, "same_values", 1,
		nil,
		[]columnParam{
			{name: "id", columnType: ColumnTypeString},
			{name: "category", columnType: ColumnTypeString},
		},
	)

	insertRecords(t, env, "same_values",
		[][][]byte{
			{[]byte("1"), []byte("Fruit")},
			{[]byte("2"), []byte("Fruit")},
			{[]byte("3"), []byte("Fruit")},
		},
	)

	return env
}

// setupSingleRecordTable はストレージを初期化し、1 レコードのみのテーブルを作成する
//
// テーブル: single (id, name)
//
//	| id  | name  |
//	| --- | ----- |
//	| 1   | Alice |
func setupSingleRecordTable(t *testing.T) *testEnv {
	t.Helper()

	env := setupTestEnv(t)

	createTable(t, env, "single", 1,
		nil,
		[]columnParam{
			{name: "id", columnType: ColumnTypeString},
			{name: "name", columnType: ColumnTypeString},
		},
	)

	insertRecords(t, env, "single",
		[][][]byte{
			{[]byte("1"), []byte("Alice")},
		},
	)

	return env
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
func setupMultiIndexTable(t *testing.T) *testEnv {
	t.Helper()

	env := setupTestEnv(t)

	createTable(t, env, "multi_idx", 1,
		[]indexParam{
			{name: "idx_name", colName: "name", secondaryKey: 1, unique: true},
			{name: "idx_email", colName: "email", secondaryKey: 2},
		},
		[]columnParam{
			{name: "id", columnType: ColumnTypeString},
			{name: "name", columnType: ColumnTypeString},
			{name: "email", columnType: ColumnTypeString},
		},
	)

	insertRecords(t, env, "multi_idx",
		[][][]byte{
			{[]byte("1"), []byte("Alice"), []byte("alice@test")},
			{[]byte("2"), []byte("Bob"), []byte("bob@test")},
		},
	)

	return env
}

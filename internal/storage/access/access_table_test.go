package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	t.Run("カタログからテーブルを開ける", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)

		// WHEN
		table, err := NewTable(env.bp, env.ct, "users")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, table)
		assert.NotNil(t, table.primaryIndex)
	})

	t.Run("セカンダリインデックスも復元される", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)

		// WHEN
		table, err := NewTable(env.bp, env.ct, "users")

		// THEN
		assert.NoError(t, err)
		assert.Len(t, table.secondaryIndexes, 2) // idx_name, idx_email
	})

	t.Run("存在しないテーブル名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)

		// WHEN
		_, err := NewTable(env.bp, env.ct, "nonexistent")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("プライマリインデックスが未登録のテーブルを開くとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnvWithoutPrimaryIndex(t)

		// WHEN
		_, err := NewTable(env.bp, env.ct, "orders")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "primary index not found")
	})
}

func TestTableBuildValMap(t *testing.T) {
	t.Run("カラム名と値のマップを構築できる", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, "users")

		// WHEN
		m := table.buildValMap([]string{"id", "name"}, []string{"1", "Alice"})

		// THEN
		assert.Equal(t, "1", m["id"])
		assert.Equal(t, "Alice", m["name"])
	})
}

func TestTableExtractPrimaryKey(t *testing.T) {
	t.Run("テーブル定義順の先頭からプライマリキーを抽出する", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, "users")

		// WHEN
		pk := table.extractPrimaryKey([]string{"1", "Alice", "alice@example.com"})

		// THEN: pkCount=1 なので先頭 1 要素
		assert.Equal(t, []string{"1"}, pk)
	})
}

func TestTableExtractSecondaryKey(t *testing.T) {
	t.Run("keyCols と valMap からインデックス定義順の SK を抽出する", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, "users")
		valMap := table.buildValMap(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		keyCols := map[string]int{"name": 0}

		// WHEN
		colNames, values := table.extractSecondaryKey(keyCols, valMap)

		// THEN
		assert.Equal(t, []string{"name"}, colNames)
		assert.Equal(t, []string{"Alice"}, values)
	})
}

func TestTableBuildSecondaryRecord(t *testing.T) {
	t.Run("セカンダリインデックス用のレコードを構築できる", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, "users")

		// idx_name のインデックスを探す
		var si *SecondaryIndex
		for _, s := range table.secondaryIndexes {
			if s.indexName == "idx_name" {
				si = s
				break
			}
		}
		assert.NotNil(t, si)

		// WHEN
		sr, err := table.buildSecondaryRecord(si, []string{"name"}, []string{"Alice"}, []string{"1"})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, sr)
		assert.Equal(t, []string{"Alice"}, sr.Values)
		assert.Equal(t, []string{"1"}, sr.Pk)
	})
}

// tableTestEnv は Table テスト用の環境
type tableTestEnv struct {
	ct *catalog.Catalog
	bp *buffer.BufferPool
}

// setupTableTestEnv は NewTable テスト用の環境を構築する
//
// テーブル: users (id:0, name:1, email:2), PK=id, pkCount=1
// インデックス:
//   - PRIMARY: カラム (id)
//   - idx_name: NonUnique, カラム (name)
//   - idx_email: Unique, カラム (email)
func setupTableTestEnv(t *testing.T) *tableTestEnv {
	t.Helper()

	// setupIteratorTestEnv と同じバッファプール + カタログを使う
	env := setupIteratorTestEnv(t)

	fileId := page.FileId(2)

	// テーブルメタデータ (MetaPageId としてプライマリ B+Tree の MetaPageId を使用)
	_ = env.ct.TableMeta.Insert("users", env.primaryTree.MetaPageId, 3)

	// プライマリインデックスメタデータ
	piIndexId := catalog.IndexId(0)
	_ = env.ct.IndexMeta.Insert(
		fileId,
		catalog.PrimaryIndexName,
		piIndexId,
		catalog.IndexTypePrimary,
		1,
		env.primaryTree.MetaPageId,
	)
	_ = env.ct.IndexKeyColMeta.Insert(piIndexId, "id", 0)

	// セカンダリインデックス idx_name のメタデータ (B+Tree は secondaryTree を再利用)
	siNameId := catalog.IndexId(1)
	_ = env.ct.IndexMeta.Insert(
		fileId,
		"idx_name",
		siNameId,
		catalog.IndexTypeNonUnique,
		1,
		env.secondaryTree.MetaPageId,
	)
	_ = env.ct.IndexKeyColMeta.Insert(siNameId, "name", 0)

	// セカンダリインデックス idx_email のメタデータ (新しい B+Tree が必要)
	siEmailTree, err := btree.CreateBtree(env.bp, fileId)
	if err != nil {
		t.Fatalf("idx_email B+Tree の作成に失敗: %v", err)
	}
	siEmailId := catalog.IndexId(2)
	_ = env.ct.IndexMeta.Insert(
		fileId,
		"idx_email",
		siEmailId,
		catalog.IndexTypeUnique,
		1,
		siEmailTree.MetaPageId,
	)
	_ = env.ct.IndexKeyColMeta.Insert(siEmailId, "email", 0)

	return &tableTestEnv{
		ct: env.ct,
		bp: env.bp,
	}
}

// setupTableTestEnvWithoutPrimaryIndex はテーブルメタのみ登録し、プライマリインデックスを登録しない環境を構築する
func setupTableTestEnvWithoutPrimaryIndex(t *testing.T) *tableTestEnv {
	t.Helper()

	env := setupIteratorTestEnv(t)

	// テーブルメタデータのみ登録 (プライマリインデックスなし)
	_ = env.ct.TableMeta.Insert("orders", env.primaryTree.MetaPageId, 2)

	return &tableTestEnv{
		ct: env.ct,
		bp: env.bp,
	}
}

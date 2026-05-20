package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/stretchr/testify/assert"
)

const tableTrxId lock.TrxId = 1

func TestTableUpdate(t *testing.T) {
	t.Run("非キーカラムをインプレース更新できる", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)

		// WHEN
		err := table.Update(before, []string{"name"}, []string{"Bob"}, tableTrxId)

		// THEN
		assert.NoError(t, err)
		updated := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", updated.Values[1])
		assert.Equal(t, "alice@example.com", updated.Values[2])
	})

	t.Run("セカンダリインデックスのカラムを更新すると旧 SK が論理削除され新 SK が挿入される", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)

		// WHEN
		err := table.Update(before, []string{"name"}, []string{"Bob"}, tableTrxId)

		// THEN
		assert.NoError(t, err)
		idxName := findSecondaryIndex(t, table, "idx_name")
		iter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Bob", result.Values[1])
	})

	t.Run("セカンダリインデックスに影響しないカラムの更新ではインデックスが変更されない", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)

		// WHEN
		err := table.Update(before, []string{"email"}, []string{"new@example.com"}, tableTrxId)

		// THEN
		assert.NoError(t, err)
		idxName := findSecondaryIndex(t, table, "idx_name")
		iter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.Values[1])
		assert.Equal(t, "new@example.com", result.Values[2])
	})

	t.Run("複数のセカンダリインデックスのうち影響するものだけが更新される", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)

		// WHEN
		err := table.Update(before, []string{"name"}, []string{"Charlie"}, tableTrxId)

		// THEN
		assert.NoError(t, err)
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Charlie", nameResult.Values[1])
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.Search(SearchModeStart{})
		assert.NoError(t, err)
		emailResult, ok, err := emailIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Charlie", emailResult.Values[1])
		assert.Equal(t, "alice@example.com", emailResult.Values[2])
	})

	t.Run("存在しないカラムで更新するとエラーを返す", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)

		// WHEN
		err := table.Update(before, []string{"nonexistent"}, []string{"val"}, tableTrxId)

		// THEN
		assert.Error(t, err)
	})
}

func TestTableIsIndexAffected(t *testing.T) {
	t.Run("インデックスカラムが更新対象に含まれる場合は true を返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
		keyCols := map[string]int{"name": 0}
		updatedCols := map[string]string{"name": "Bob"}

		// WHEN
		result := table.isIndexAffected(keyCols, updatedCols)

		// THEN
		assert.True(t, result)
	})

	t.Run("インデックスカラムが更新対象に含まれない場合は false を返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
		keyCols := map[string]int{"name": 0}
		updatedCols := map[string]string{"email": "new@example.com"}

		// WHEN
		result := table.isIndexAffected(keyCols, updatedCols)

		// THEN
		assert.False(t, result)
	})

	t.Run("空の keyCols では false を返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
		keyCols := map[string]int{}
		updatedCols := map[string]string{"name": "Bob"}

		// WHEN
		result := table.isIndexAffected(keyCols, updatedCols)

		// THEN
		assert.False(t, result)
	})
}

// setupTableWithRecord はテーブルにレコード 1 件を挿入した状態の Table を返す
func setupTableWithRecord(t *testing.T) *Table {
	t.Helper()
	env := setupTableTestEnv(t)
	table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
	if err != nil {
		t.Fatalf("Table の作成に失敗: %v", err)
	}
	if err := table.Insert(
		[]string{"id", "name", "email"},
		[]string{"1", "Alice", "alice@example.com"},
		tableTrxId,
	); err != nil {
		t.Fatalf("レコードの挿入に失敗: %v", err)
	}
	return table
}

// searchFirstPrimaryRecord はプライマリインデックスの先頭レコードを返す
func searchFirstPrimaryRecord(t *testing.T, table *Table) *PrimaryRecord {
	t.Helper()
	iter, err := table.primaryIndex.Search(SearchModeStart{})
	if err != nil {
		t.Fatalf("プライマリインデックスの検索に失敗: %v", err)
	}
	record, ok, err := iter.Next()
	if err != nil {
		t.Fatalf("レコードの取得に失敗: %v", err)
	}
	if !ok {
		t.Fatal("レコードが見つからない")
	}
	return record
}

// findSecondaryIndex は指定した名前のセカンダリインデックスを返す
func findSecondaryIndex(t *testing.T, table *Table, name string) *SecondaryIndex {
	t.Helper()
	for _, si := range table.secondaryIndexes {
		if si.indexName == name {
			return si
		}
	}
	t.Fatalf("セカンダリインデックス %q が見つからない", name)
	return nil
}

package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestTableInsert(t *testing.T) {
	t.Run("テーブルにレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			tableTrxId,
		)

		// THEN
		assert.NoError(t, err)
		record := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.Values)
	})

	t.Run("セカンダリインデックスにも挿入される", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)

		// THEN
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.Values[1])
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.Search(SearchModeStart{})
		assert.NoError(t, err)
		emailResult, ok, err := emailIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "alice@example.com", emailResult.Values[2])
	})

	t.Run("異なるプライマリキーで複数レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)

		// WHEN
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
			tableTrxId,
		)

		// THEN
		assert.NoError(t, err)
		first := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Alice", first.Values[1])
	})

	t.Run("同一プライマリキーで挿入すると ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t)

		// WHEN
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Bob", "bob@example.com"},
			tableTrxId,
		)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("論理削除済みの同一プライマリキーに再挿入できる", func(t *testing.T) {
		// GIVEN
		table := setupTableWithRecord(t) // id=1, Alice
		record := searchFirstPrimaryRecord(t, table)
		err := table.SoftDelete(record, tableTrxId)
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Charlie", "charlie@example.com"},
			tableTrxId,
		)

		// THEN
		assert.NoError(t, err)
		reinserted := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Charlie", reinserted.Values[1])
	})

	t.Run("カラム数が不足しているとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			[]string{"id", "name"},
			[]string{"1", "Alice"},
			tableTrxId,
		)

		// THEN
		assert.Error(t, err)
	})

	t.Run("カラム順がテーブル定義順と異なる場合でもセカンダリインデックスに正しい PK が格納される", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
		assert.NoError(t, err)

		// WHEN (テーブル定義順: id, name, email だが name, email, id の順で指定)
		err = table.Insert(
			[]string{"name", "email", "id"},
			[]string{"Alice", "alice@example.com", "1"},
			tableTrxId,
		)

		// THEN
		assert.NoError(t, err)

		// プライマリインデックスのレコードがテーブル定義順で格納されている
		record := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.Values)

		// セカンダリインデックスからプライマリキー "1" で検索できる
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.Search(SearchModeStart{})
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.Values[1])
		assert.Equal(t, "1", nameResult.Values[0])
	})

	t.Run("挿入後に rollPtr が設定される", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			tableTrxId,
		)

		// THEN
		assert.NoError(t, err)
		record := searchFirstPrimaryRecord(t, table)
		assert.NotEqual(t, undo.NullPointer, record.rollPtr)
	})

	t.Run("ユニークセカンダリインデックスに重複値を挿入するとエラーを返す", func(t *testing.T) {
		// GIVEN (idx_email は Unique)
		table := setupTableWithRecord(t)

		// WHEN (email が重複)
		err := table.Insert(
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "alice@example.com"},
			tableTrxId,
		)

		// THEN
		assert.Error(t, err)
	})
}

package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestTableInsert(t *testing.T) {
	t.Run("テーブルにレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
		assert.NoError(t, err)
		trx := env.trxMgr.Begin()

		// WHEN
		err = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		record := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.values)
	})

	t.Run("セカンダリインデックスにも挿入される", func(t *testing.T) {
		// GIVEN
		table, _, _ := setupTableWithRecord(t)

		// THEN
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.values[1])
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		emailResult, ok, err := emailIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "alice@example.com", emailResult.values[2])
	})

	t.Run("プライマリレコードの lastTrxId に挿入した trxId が記録される", func(t *testing.T) {
		// GIVEN
		table, _, insertedTrxId := setupTableWithRecord(t)

		// WHEN
		record := searchFirstPrimaryRecord(t, table)

		// THEN
		assert.Equal(t, byte(0), record.deleteMark)
		assert.Equal(t, insertedTrxId, record.lastTrxId)
	})

	t.Run("セカンダリインデックスの lastTrxId に挿入した trxId が記録される", func(t *testing.T) {
		// GIVEN
		table, _, insertedTrxId := setupTableWithRecord(t)

		// WHEN
		nameRec := findSecondaryRecordByValue(t, table, "idx_name", "Alice")
		emailRec := findSecondaryRecordByValue(t, table, "idx_email", "alice@example.com")

		// THEN
		assert.NotNil(t, nameRec)
		assert.Equal(t, byte(0), nameRec.deleteMark)
		assert.Equal(t, insertedTrxId, nameRec.lastTrxId)
		assert.NotNil(t, emailRec)
		assert.Equal(t, byte(0), emailRec.deleteMark)
		assert.Equal(t, insertedTrxId, emailRec.lastTrxId)
	})

	t.Run("異なるプライマリキーで複数レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()

		// WHEN
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		first := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Alice", first.values[1])
	})

	t.Run("同一プライマリキーで挿入すると ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()

		// WHEN
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Bob", "bob@example.com"},
		)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("論理削除済みの同一プライマリキーに再挿入できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()
		record := currentReadFirst(t, table, trx)
		err := table.SoftDelete(trx, record)
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Charlie", "charlie@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		reinserted := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Charlie", reinserted.values[1])
	})

	t.Run("カラム数が不足しているとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
		assert.NoError(t, err)
		trx := env.trxMgr.Begin()

		// WHEN
		err = table.Insert(
			trx,
			[]string{"id", "name"},
			[]string{"1", "Alice"},
		)

		// THEN
		assert.Error(t, err)
	})

	t.Run("カラム順がテーブル定義順と異なる場合でもセカンダリインデックスに正しい PK が格納される", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
		assert.NoError(t, err)
		trx := env.trxMgr.Begin()

		// WHEN
		err = table.Insert(
			trx,
			[]string{"name", "email", "id"},
			[]string{"Alice", "alice@example.com", "1"},
		)

		// THEN
		assert.NoError(t, err)

		record := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.values)

		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", nameResult.values[1])
		assert.Equal(t, "1", nameResult.values[0])
	})

	t.Run("挿入後に rollPtr が設定される", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
		assert.NoError(t, err)
		trx := env.trxMgr.Begin()

		// WHEN
		err = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		record := searchFirstPrimaryRecord(t, table)
		assert.NotEqual(t, undo.NullPointer(), record.rollPtr)
	})

	t.Run("ユニークセカンダリインデックスに重複値を挿入するとエラーを返す", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()

		// WHEN
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "alice@example.com"},
		)

		// THEN
		assert.Error(t, err)
	})

	t.Run("FK 制約に違反する挿入は ErrForeignKeyViolation を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		fkTx := env.trxMgr.Begin()

		// WHEN
		err := env.child.Insert(fkTx, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "999"})

		// THEN
		assert.ErrorIs(t, err, ErrForeignKeyViolation)
	})
}

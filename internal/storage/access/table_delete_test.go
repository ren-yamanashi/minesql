package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestTableSoftDelete(t *testing.T) {
	t.Run("プライマリインデックスからレコードが論理削除される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()
		record := currentReadFirst(t, table, trx)

		// WHEN
		err := table.SoftDelete(trx, record)

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("セカンダリインデックスからもレコードが論理削除される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()
		record := currentReadFirst(t, table, trx)

		// WHEN
		err := table.SoftDelete(trx, record)

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err = emailIter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("SoftDelete 後、セカンダリレコードの lastTrxId に削除した trxId が記録される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()
		record := currentReadFirst(t, table, trx)

		// WHEN
		err := table.SoftDelete(trx, record)

		// THEN
		assert.NoError(t, err)
		nameRec := findSecondaryRecordByValue(t, table, "idx_name", "Alice")
		emailRec := findSecondaryRecordByValue(t, table, "idx_email", "alice@example.com")
		assert.NotNil(t, nameRec)
		assert.Equal(t, byte(1), nameRec.deleteMark)
		assert.Equal(t, trx.trxId, nameRec.lastTrxId)
		assert.NotNil(t, emailRec)
		assert.Equal(t, byte(1), emailRec.deleteMark)
		assert.Equal(t, trx.trxId, emailRec.lastTrxId)
	})

	t.Run("論理削除後に同一プライマリキーで再挿入できる", func(t *testing.T) {
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
			[]string{"1", "Bob", "bob@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		reinserted := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", reinserted.values[1])
		assert.Equal(t, "bob@example.com", reinserted.values[2])
	})

	t.Run("論理削除後のレコードに rollPtr が設定される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()
		record := currentReadFirst(t, table, trx)

		// WHEN
		err := table.SoftDelete(trx, record)

		// THEN
		assert.NoError(t, err)

		encodedRecord := record.Encode()
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		existing, _, err := table.primaryIndex.tree.FindByKey(mtr, encodedRecord.Key())
		assert.NoError(t, err)
		decoded, err := DecodePrimaryRecord(existing, table.catalog, table.bufferPool, table.primaryIndex.fileId())
		assert.NoError(t, err)
		assert.NotEqual(t, undo.NullPointer(), decoded.rollPtr)
	})

	t.Run("存在しないレコードを論理削除するとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
		assert.NoError(t, err)
		fakeRecord := &PrimaryRecord{
			pkCount:    1,
			deleteMark: 0,
			rollPtr:    undo.NullPointer(),
			colNames:   []string{"id", "name", "email"},
			values:     []string{"999", "Nobody", "nobody@example.com"},
		}
		trx := env.trxMgr.Begin()

		// WHEN
		err = table.SoftDelete(trx, fakeRecord)

		// THEN
		assert.Error(t, err)
	})

	t.Run("複数レコードのうち 1 件だけ論理削除できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		)
		assert.NoError(t, err)
		alice := currentReadFirst(t, table, trx)
		assert.Equal(t, "Alice", alice.values[1])

		// WHEN
		err = table.SoftDelete(trx, alice)

		// THEN
		assert.NoError(t, err)
		remaining := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", remaining.values[1])
		assert.Equal(t, "bob@example.com", remaining.values[2])
	})

	t.Run("子テーブルから参照されているレコードの論理削除は ErrForeignKeyViolation を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		fkTx := env.trxMgr.Begin()
		_ = env.parent.Insert(fkTx, []string{"id", "name"}, []string{"1", "Sales"})
		_ = env.child.Insert(fkTx, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"})
		record := currentReadFirst(t, env.parent, fkTx)

		// WHEN
		err := env.parent.SoftDelete(fkTx, record)

		// THEN
		assert.ErrorIs(t, err, ErrForeignKeyViolation)
	})
}

func TestTableDelete(t *testing.T) {
	t.Run("プライマリインデックスからレコードが物理削除される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		record := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Delete(trx, record)

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("セカンダリインデックスからもレコードが物理削除される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		record := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Delete(trx, record)

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		_, ok, err = emailIter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("物理削除後に同一プライマリキーで再挿入できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		record := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()
		err := table.Delete(trx, record)
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Bob", "bob@example.com"},
		)

		// THEN
		assert.NoError(t, err)
		reinserted := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", reinserted.values[1])
		assert.Equal(t, "bob@example.com", reinserted.values[2])
	})

	t.Run("存在しないレコードを物理削除するとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
		assert.NoError(t, err)
		fakeRecord := &PrimaryRecord{
			pkCount:    1,
			deleteMark: 0,
			rollPtr:    undo.NullPointer(),
			colNames:   []string{"id", "name", "email"},
			values:     []string{"999", "Nobody", "nobody@example.com"},
		}
		trx := env.trxMgr.Begin()

		// WHEN
		err = table.Delete(trx, fakeRecord)

		// THEN
		assert.Error(t, err)
	})

	t.Run("複数レコードのうち 1 件だけ物理削除できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		trx := tm.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"2", "Bob", "bob@example.com"},
		)
		assert.NoError(t, err)
		alice := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Alice", alice.values[1])

		// WHEN
		err = table.Delete(trx, alice)

		// THEN
		assert.NoError(t, err)
		remaining := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", remaining.values[1])
		assert.Equal(t, "bob@example.com", remaining.values[2])
	})
}

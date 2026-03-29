package executor

import (
	"minesql/internal/access"
	"minesql/internal/catalog"
	"minesql/internal/engine"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBegin(t *testing.T) {
	t.Run("トランザクションが ACTIVE 状態で開始される", func(t *testing.T) {
		trx := Begin(1)

		assert.Equal(t, TrxId(1), trx.trxId)
		assert.Equal(t, TRX_ACTIVE, trx.state)
		assert.Equal(t, 0, len(trx.undoLogRecords))
	})
}

func TestCommit(t *testing.T) {
	t.Run("Commit するとトランザクションが INACTIVE になり Undo ログがクリアされる", func(t *testing.T) {
		initStorageManagerForTest(t)
		defer engine.Reset()

		trx := Begin(0)
		tbl := setupTransactionTestTable(t)

		// Insert して Undo ログが蓄積される
		ins := NewInsert(trx, tbl, []Record{
			{[]byte("a"), []byte("Alice")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(trx.undoLogRecords))

		// WHEN
		trx.Commit()

		// THEN
		assert.Equal(t, TRX_INACTIVE, trx.state)
		assert.Equal(t, 0, len(trx.undoLogRecords))

		// Commit 後もデータは残っている
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, 1, len(recs))
	})
}

func TestRollback(t *testing.T) {
	t.Run("Insert を Rollback すると行が物理削除される", func(t *testing.T) {
		initStorageManagerForTest(t)
		defer engine.Reset()

		trx := Begin(0)
		tbl := setupTransactionTestTable(t)

		ins := NewInsert(trx, tbl, []Record{
			{[]byte("a"), []byte("Alice")},
			{[]byte("b"), []byte("Bob")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)

		// WHEN
		err = trx.Rollback()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, TRX_INACTIVE, trx.state)
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, 0, len(recs))
	})

	t.Run("Delete を Rollback すると行が復元される", func(t *testing.T) {
		initStorageManagerForTest(t)
		defer engine.Reset()

		tbl := setupTransactionTestTable(t)

		// 先にデータを挿入して Commit
		insertTrx := Begin(0)
		ins := NewInsert(insertTrx, tbl, []Record{
			{[]byte("a"), []byte("Alice")},
			{[]byte("b"), []byte("Bob")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)
		insertTrx.Commit()

		// Delete トランザクション
		deleteTrx := Begin(1)
		del := NewDelete(deleteTrx, tbl, NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))
		_, err = del.Next()
		assert.NoError(t, err)

		// WHEN
		err = deleteTrx.Rollback()

		// THEN
		assert.NoError(t, err)
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, 2, len(recs))
	})

	t.Run("Update を Rollback すると行が元の値に戻る", func(t *testing.T) {
		initStorageManagerForTest(t)
		defer engine.Reset()

		tbl := setupTransactionTestTable(t)

		// 先にデータを挿入して Commit
		insertTrx := Begin(0)
		ins := NewInsert(insertTrx, tbl, []Record{
			{[]byte("a"), []byte("Alice")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)
		insertTrx.Commit()

		// Update トランザクション
		updateTrx := Begin(1)
		upd := NewUpdate(updateTrx, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Carol")},
		}, NewTableScan(
			tbl,
			access.RecordSearchModeStart{},
			func(record Record) bool { return true },
		))
		_, err = upd.Next()
		assert.NoError(t, err)

		// 更新後の確認
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, "Carol", string(recs[0][1]))

		// WHEN
		err = updateTrx.Rollback()

		// THEN
		assert.NoError(t, err)
		recs = collectAllRecords(t, tbl)
		assert.Equal(t, 1, len(recs))
		assert.Equal(t, "Alice", string(recs[0][1]))
	})

	t.Run("複数操作を含むトランザクションの Rollback", func(t *testing.T) {
		initStorageManagerForTest(t)
		defer engine.Reset()

		tbl := setupTransactionTestTable(t)

		// 先にデータを挿入して Commit
		insertTrx := Begin(0)
		ins := NewInsert(insertTrx, tbl, []Record{
			{[]byte("a"), []byte("Alice")},
			{[]byte("b"), []byte("Bob")},
		})
		_, err := ins.Next()
		assert.NoError(t, err)
		insertTrx.Commit()

		// 1 つのトランザクション内で Insert + Update + Delete
		trx := Begin(1)

		// Insert
		ins2 := NewInsert(trx, tbl, []Record{
			{[]byte("c"), []byte("Carol")},
		})
		_, err = ins2.Next()
		assert.NoError(t, err)

		// Update: Alice → Dave
		upd := NewUpdate(trx, tbl, []SetColumn{
			{Pos: 1, Value: []byte("Dave")},
		}, NewTableScan(
			tbl,
			access.RecordSearchModeKey{Key: [][]byte{[]byte("a")}},
			func(record Record) bool { return string(record[0]) == "a" },
		))
		_, err = upd.Next()
		assert.NoError(t, err)

		// Delete: Bob
		del := NewDelete(trx, tbl, NewFilter(
			NewTableScan(
				tbl,
				access.RecordSearchModeStart{},
				func(record Record) bool { return true },
			),
			func(record Record) bool { return string(record[0]) == "b" },
		))
		_, err = del.Next()
		assert.NoError(t, err)

		// WHEN
		err = trx.Rollback()

		// THEN: 初期状態に戻る
		assert.NoError(t, err)
		recs := collectAllRecords(t, tbl)
		assert.Equal(t, 2, len(recs))
		assert.Equal(t, "Alice", string(recs[0][1]))
		assert.Equal(t, "Bob", string(recs[1][1]))
	})
}

func setupTransactionTestTable(t *testing.T) *access.TableAccessMethod {
	t.Helper()
	createTableForTest(t, "test_trx", nil, []*ColumnParam{
		{Name: "id", Type: catalog.ColumnTypeString},
		{Name: "name", Type: catalog.ColumnTypeString},
	})
	tbl, err := getTableAccessMethod("test_trx")
	assert.NoError(t, err)
	return tbl
}

func collectAllRecords(t *testing.T, tbl *access.TableAccessMethod) []Record {
	t.Helper()
	scan := NewTableScan(
		tbl,
		access.RecordSearchModeStart{},
		func(record Record) bool { return true },
	)
	recs, err := fetchAll(scan)
	assert.NoError(t, err)
	return recs
}

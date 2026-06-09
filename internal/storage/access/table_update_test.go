package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestTableUpdate(t *testing.T) {
	t.Run("非キーカラムをインプレース更新できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)
		updated := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "Bob", updated.values[1])
		assert.Equal(t, "alice@example.com", updated.values[2])
	})

	t.Run("セカンダリインデックスのカラムを更新すると旧 SK が論理削除され新 SK が挿入される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		iter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Bob", result.values[1])
	})

	t.Run("SK 変更 UPDATE 後、旧 SK と新 SK の両方の lastTrxId に UPDATE した trxId が記録される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)
		records := fetchAllSecondaryRecords(t, table, "idx_name")
		var oldRec, newRec *SecondaryRecord
		for _, r := range records {
			if r.values[0] == "Alice" {
				oldRec = r
				continue
			}
			if r.values[0] == "Bob" {
				newRec = r
			}
		}
		assert.NotNil(t, oldRec)
		assert.Equal(t, byte(1), oldRec.deleteMark)
		assert.Equal(t, trx.trxId, oldRec.lastTrxId)
		assert.NotNil(t, newRec)
		assert.Equal(t, byte(0), newRec.deleteMark)
		assert.Equal(t, trx.trxId, newRec.lastTrxId)
	})

	t.Run("セカンダリインデックスに影響しないカラムの更新ではインデックスが変更されない", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"email"}, []string{"new@example.com"})

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		iter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
		assert.Equal(t, "new@example.com", result.values[2])
	})

	t.Run("複数のセカンダリインデックスのうち影響するものだけが更新される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"name"}, []string{"Charlie"})

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Charlie", nameResult.values[1])
		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		emailResult, ok, err := emailIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Charlie", emailResult.values[1])
		assert.Equal(t, "alice@example.com", emailResult.values[2])
	})

	t.Run("存在しないカラムで更新するとエラーを返す", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"nonexistent"}, []string{"val"})

		// THEN
		assert.Error(t, err)
	})

	t.Run("複数カラムを同時に更新できる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"name", "email"}, []string{"Bob", "bob@example.com"})

		// THEN
		assert.NoError(t, err)
		updated := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "1", updated.values[0])
		assert.Equal(t, "Bob", updated.values[1])
		assert.Equal(t, "bob@example.com", updated.values[2])
	})

	t.Run("複数カラムの更新で全セカンダリインデックスが更新される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"name", "email"}, []string{"Bob", "bob@example.com"})

		// THEN
		assert.NoError(t, err)

		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		nameIter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		nameResult, ok, err := nameIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Bob", nameResult.values[1])

		idxEmail := findSecondaryIndex(t, table, "idx_email")
		emailIter, err := idxEmail.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		emailResult, ok, err := emailIter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "bob@example.com", emailResult.values[2])
	})

	t.Run("PK カラムを更新すると論理削除 + 新規挿入で処理される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"id"}, []string{"2"})

		// THEN
		assert.NoError(t, err)
		updated := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "2", updated.values[0])
		assert.Equal(t, "Alice", updated.values[1])
		assert.Equal(t, "alice@example.com", updated.values[2])
	})

	t.Run("PK カラムを同じ値で更新するとインプレース更新になる", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"id", "name"}, []string{"1", "Bob"})

		// THEN
		assert.NoError(t, err)
		updated := searchFirstPrimaryRecord(t, table)
		assert.Equal(t, "1", updated.values[0])
		assert.Equal(t, "Bob", updated.values[1])
	})

	t.Run("PK 更新時にセカンダリインデックスも更新される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"id"}, []string{"2"})

		// THEN
		assert.NoError(t, err)
		mtr := buffer.NewMtr(table.bufferPool)
		defer mtr.UnpinAll()
		idxName := findSecondaryIndex(t, table, "idx_name")
		iter, err := idxName.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
	})

	t.Run("更新後のレコードに rollPtr が設定される", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()

		// WHEN
		err := table.Update(trx, before, []string{"name"}, []string{"Bob"})

		// THEN
		assert.NoError(t, err)
		updated := searchFirstPrimaryRecord(t, table)
		assert.NotEqual(t, undo.NullPointer(), updated.rollPtr)
	})

	t.Run("FK カラムを存在しない値に更新すると ErrForeignKeyViolation を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		fkTx := env.trxMgr.Begin()
		_ = env.parent.Insert(fkTx, []string{"id", "name"}, []string{"1", "Sales"})
		_ = env.child.Insert(fkTx, []string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"})
		record := searchFirstPrimaryRecord(t, env.child)

		// WHEN
		err := env.child.Update(fkTx, record, []string{"dept_id"}, []string{"999"})

		// THEN
		assert.ErrorIs(t, err, ErrForeignKeyViolation)
	})
}

func TestTableIsPrimaryKeyChanged(t *testing.T) {
	t.Run("PK の値が異なる場合は true を返す", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()
		after, _ := before.update(trx.trxId, []string{"id"}, []string{"2"})

		// WHEN
		result := table.isPrimaryKeyChanged(before, after)

		// THEN
		assert.True(t, result)
	})

	t.Run("PK の値が同じ場合は false を返す", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()
		after, _ := before.update(trx.trxId, []string{"name"}, []string{"Bob"})

		// WHEN
		result := table.isPrimaryKeyChanged(before, after)

		// THEN
		assert.False(t, result)
	})

	t.Run("PK カラムを同じ値で更新した場合は false を返す", func(t *testing.T) {
		// GIVEN
		table, tm, _ := setupTableWithRecord(t)
		before := searchFirstPrimaryRecord(t, table)
		trx := tm.Begin()
		after, _ := before.update(trx.trxId, []string{"id"}, []string{"1"})

		// WHEN
		result := table.isPrimaryKeyChanged(before, after)

		// THEN
		assert.False(t, result)
	})
}

func TestTableIsIndexAffected(t *testing.T) {
	t.Run("インデックスカラムが更新対象に含まれる場合は true を返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
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
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
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
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
		keyCols := map[string]int{}
		updatedCols := map[string]string{"name": "Bob"}

		// WHEN
		result := table.isIndexAffected(keyCols, updatedCols)

		// THEN
		assert.False(t, result)
	})
}

// setupTableWithRecord はテーブルにレコード 1 件を挿入した状態の Table, TrxManager, 挿入に使った TrxId を返す
func setupTableWithRecord(t *testing.T) (*Table, *TrxManager, lock.TrxId) {
	t.Helper()
	env := setupTableTestEnv(t)
	table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, env.redoLog, "users")
	if err != nil {
		t.Fatalf("Table の作成に失敗: %v", err)
	}
	trx := env.trxMgr.Begin()
	if err := table.Insert(
		trx,
		[]string{"id", "name", "email"},
		[]string{"1", "Alice", "alice@example.com"},
	); err != nil {
		t.Fatalf("レコードの挿入に失敗: %v", err)
	}
	insertedTrxId := trx.trxId
	if err := env.trxMgr.Commit(trx); err != nil {
		t.Fatalf("コミットに失敗: %v", err)
	}
	return table, env.trxMgr, insertedTrxId
}

// searchFirstPrimaryRecord はプライマリインデックスの先頭レコードを返す
func searchFirstPrimaryRecord(t *testing.T, table *Table) *PrimaryRecord {
	t.Helper()
	mtr := buffer.NewMtr(table.bufferPool)
	defer mtr.UnpinAll()
	iter, err := table.primaryIndex.search(mtr, SearchModeStart{}, nil)
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
func findSecondaryIndex(t *testing.T, table *Table, name string) *secondaryIndex {
	t.Helper()
	for _, si := range table.secondaryIndexes {
		if si.indexName == name {
			return si
		}
	}
	t.Fatalf("セカンダリインデックス %q が見つからない", name)
	return nil
}

// fetchAllSecondaryRecords はセカンダリ B+Tree から削除マークを問わず全レコードを取得する
func fetchAllSecondaryRecords(t *testing.T, table *Table, indexName string) []*SecondaryRecord {
	t.Helper()
	si := findSecondaryIndex(t, table, indexName)
	mtr := buffer.NewMtr(table.bufferPool)
	defer mtr.UnpinAll()
	iter, err := si.tree.Search(mtr, SearchModeStart{}.Encode())
	if err != nil {
		t.Fatalf("セカンダリ B+Tree の検索に失敗: %v", err)
	}
	defer iter.Close()

	var records []*SecondaryRecord
	for {
		record, ok, err := iter.Next()
		if err != nil {
			t.Fatalf("セカンダリレコードの取得に失敗: %v", err)
		}
		if !ok {
			break
		}
		sr, err := DecodeSecondaryRecord(record, table.catalog, table.bufferPool, si.fileId, indexName)
		if err != nil {
			t.Fatalf("セカンダリレコードのデコードに失敗: %v", err)
		}
		records = append(records, sr)
	}
	return records
}

// findSecondaryRecordByValue は指定したセカンダリインデックスの中で values[0] が一致する最初のレコードを返す
func findSecondaryRecordByValue(t *testing.T, table *Table, indexName, value string) *SecondaryRecord {
	t.Helper()
	for _, r := range fetchAllSecondaryRecords(t, table, indexName) {
		if len(r.values) > 0 && r.values[0] == value {
			return r
		}
	}
	return nil
}

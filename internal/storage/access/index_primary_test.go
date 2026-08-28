package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

const testTrxId lock.TrxId = 1

func TestNewPrimaryIndex(t *testing.T) {
	t.Run("既存のプライマリインデックスを開ける", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		lockMgr := lock.NewManager()
		created, err := createPrimaryIndex(env.trxMgr.BeginDDL(), page.FileId(2), 1)
		assert.NoError(t, err)

		// WHEN
		pi := newPrimaryIndex(env.ct, env.bp, created.tree.MetaPageId(), 1, lockMgr, nil)

		// THEN
		assert.NotNil(t, pi)
	})
}

func TestCreatePrimaryIndex(t *testing.T) {
	t.Run("プライマリインデックスを新規作成できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)

		// WHEN
		pi, err := createPrimaryIndex(env.trxMgr.BeginDDL(), page.FileId(2), 1)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, pi)
		assert.Equal(t, 1, pi.pkCount)
	})

	t.Run("pkCount が複数のプライマリインデックスを作成できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)

		// WHEN
		pi, err := createPrimaryIndex(env.trxMgr.BeginDDL(), page.FileId(2), 2)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, pi.pkCount)
	})
}

func TestPrimaryIndexSearch(t *testing.T) {
	t.Run("全件スキャンでレコードを取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		writeMtr := buffer.NewMtr(pi.bufferPool)
		record := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(writeMtr, record, testTrxId)
		writeMtr.UnpinAll()

		// WHEN
		iter, err := pi.search(SearchModeStart{}, nil)

		// THEN
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, result.values)
	})

	t.Run("空のインデックスを検索するとデータなしを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		iter, err := pi.search(SearchModeStart{}, nil)
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestPrimaryIndexInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		mtr := buffer.NewMtr(pi.bufferPool)
		defer mtr.UnpinAll()
		record := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")

		// WHEN
		err := pi.insert(mtr, record, testTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一プライマリキーの重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		mtr := buffer.NewMtr(pi.bufferPool)
		defer mtr.UnpinAll()
		r1 := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.insert(mtr, r1, testTrxId)
		r2 := buildTestPrimaryRecord(t, pi, "1", "Bob", "b@example.com")

		// WHEN
		err := pi.insert(mtr, r2, testTrxId)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("異なるプライマリキーであれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		mtr := buffer.NewMtr(pi.bufferPool)
		defer mtr.UnpinAll()
		r1 := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.insert(mtr, r1, testTrxId)
		r2 := buildTestPrimaryRecord(t, pi, "2", "Bob", "b@example.com")

		// WHEN
		err := pi.insert(mtr, r2, testTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("論理削除済みの同一キーがある場合は上書きできる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		insertMtr := buffer.NewMtr(pi.bufferPool)
		r1 := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.insert(insertMtr, r1, testTrxId)
		insertMtr.UnpinAll()

		// 論理削除
		iter, _ := pi.search(SearchModeStart{}, nil)
		record, _, _ := iter.Next()
		iter.Close()
		deleteMtr := buffer.NewMtr(pi.bufferPool)
		_ = pi.softDelete(deleteMtr, record, testTrxId)
		deleteMtr.UnpinAll()

		r2 := buildTestPrimaryRecord(t, pi, "1", "Bob", "b@example.com")

		// WHEN
		reinsertMtr := buffer.NewMtr(pi.bufferPool)
		defer reinsertMtr.UnpinAll()
		err := pi.insert(reinsertMtr, r2, testTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("挿入後に排他ロックが取得される", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		mtr := buffer.NewMtr(pi.bufferPool)
		defer mtr.UnpinAll()
		record := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")

		// WHEN
		err := pi.insert(mtr, record, testTrxId)

		// THEN
		assert.NoError(t, err)
		// 同一トランザクションで既に排他ロックを保持しているので再取得は成功する
		encodedRecord := record.Encode()
		rowKey := lock.RowKey{MetaPageId: pi.tree.MetaPageId(), Key: encodedRecord.Key()}
		err = pi.lock.Lock(testTrxId, rowKey, lock.Exclusive)
		assert.NoError(t, err)
	})
}

func TestPrimaryIndexDelete(t *testing.T) {
	t.Run("レコードを物理削除できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		insertMtr := buffer.NewMtr(pi.bufferPool)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(insertMtr, r, testTrxId)
		insertMtr.UnpinAll()

		iter, _ := pi.search(SearchModeStart{}, nil)
		record, _, _ := iter.Next()
		iter.Close()

		// WHEN
		deleteMtr := buffer.NewMtr(pi.bufferPool)
		err := pi.delete(deleteMtr, record, testTrxId)
		deleteMtr.UnpinAll()

		// THEN
		assert.NoError(t, err)

		// 削除後は取得できない
		iter2, _ := pi.search(SearchModeStart{}, nil)
		_, ok, _ := iter2.Next()
		assert.False(t, ok)
	})

	t.Run("存在しないレコードの削除はエラーを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		mtr := buffer.NewMtr(pi.bufferPool)
		defer mtr.UnpinAll()
		pr := buildTestPrimaryRecord(t, pi, "999", "Nobody", "no@example.com")

		// WHEN
		err := pi.delete(mtr, pr, testTrxId)

		// THEN
		assert.Error(t, err)
	})
}

func TestPrimaryIndexSoftDelete(t *testing.T) {
	t.Run("レコードを論理削除できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		insertMtr := buffer.NewMtr(pi.bufferPool)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(insertMtr, r, testTrxId)
		insertMtr.UnpinAll()

		iter, _ := pi.search(SearchModeStart{}, nil)
		record, _, _ := iter.Next()
		iter.Close()

		// WHEN
		deleteMtr := buffer.NewMtr(pi.bufferPool)
		err := pi.softDelete(deleteMtr, record, testTrxId)
		deleteMtr.UnpinAll()

		// THEN
		assert.NoError(t, err)

		// 論理削除後は検索でスキップされる
		iter2, _ := pi.search(SearchModeStart{}, nil)
		_, ok, _ := iter2.Next()
		assert.False(t, ok)
	})

	t.Run("論理削除後に再挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		insertMtr := buffer.NewMtr(pi.bufferPool)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(insertMtr, r, testTrxId)
		insertMtr.UnpinAll()

		iter, _ := pi.search(SearchModeStart{}, nil)
		record, _, _ := iter.Next()
		iter.Close()
		deleteMtr := buffer.NewMtr(pi.bufferPool)
		_ = pi.softDelete(deleteMtr, record, testTrxId)
		deleteMtr.UnpinAll()

		r2 := buildTestPrimaryRecord(t, pi, "1", "Bob", "bob@example.com")

		// WHEN
		reinsertMtr := buffer.NewMtr(pi.bufferPool)
		defer reinsertMtr.UnpinAll()
		err := pi.insert(reinsertMtr, r2, testTrxId)

		// THEN
		assert.NoError(t, err)
	})
}

func TestPrimaryIndexUpdate(t *testing.T) {
	t.Run("レコードをインプレース更新できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		insertMtr := buffer.NewMtr(pi.bufferPool)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(insertMtr, r, testTrxId)
		insertMtr.UnpinAll()

		iter, _ := pi.search(SearchModeStart{}, nil)
		current, _, _ := iter.Next()
		iter.Close()
		newRecord, _ := current.update(testTrxId, []string{"name"}, []string{"Bob"})

		// WHEN
		updateMtr := buffer.NewMtr(pi.bufferPool)
		err := pi.update(updateMtr, newRecord, testTrxId)
		updateMtr.UnpinAll()

		// THEN
		assert.NoError(t, err)

		// 更新後の値を確認
		iter2, _ := pi.search(SearchModeStart{}, nil)
		updated, ok, _ := iter2.Next()
		assert.True(t, ok)
		assert.Equal(t, "Bob", updated.values[1])
		assert.Equal(t, "alice@example.com", updated.values[2])
	})

	t.Run("存在しないカラムで更新するとエラーを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		insertMtr := buffer.NewMtr(pi.bufferPool)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.insert(insertMtr, r, testTrxId)
		insertMtr.UnpinAll()

		iter, _ := pi.search(SearchModeStart{}, nil)
		current, _, _ := iter.Next()
		iter.Close()

		// WHEN
		_, err := current.update(testTrxId, []string{"nonexistent"}, []string{"val"})

		// THEN
		assert.Error(t, err)
	})
}

func TestPrimaryIndexFileId(t *testing.T) {
	t.Run("テーブルの FileId を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		fileId := pi.fileId()

		// THEN
		assert.Equal(t, page.FileId(2), fileId)
	})
}

func TestPrimaryIndexLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		mtr := buffer.NewMtr(pi.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		count, err := pi.leafPageCount(mtr)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestPrimaryIndexHeight(t *testing.T) {
	t.Run("ツリーの高さを取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		mtr := buffer.NewMtr(pi.bufferPool)
		defer mtr.UnpinAll()

		// WHEN
		height, err := pi.height(mtr)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// setupTestPrimaryIndex はテスト用の PrimaryIndex (pkCount=1) を作成する
func setupTestPrimaryIndex(t *testing.T) *primaryIndex {
	t.Helper()
	env := setupIteratorTestEnv(t)
	pi, err := createPrimaryIndex(env.trxMgr.BeginDDL(), page.FileId(2), 1)
	if err != nil {
		t.Fatalf("PrimaryIndex の作成に失敗: %v", err)
	}
	return pi
}

// buildTestPrimaryRecord はテスト用の PrimaryRecord (id, name, email) を構築する
func buildTestPrimaryRecord(t *testing.T, pi *primaryIndex, id, name, email string) *PrimaryRecord {
	t.Helper()
	pr, err := NewPrimaryRecord(pi.catalog, pi.bufferPool, NewPrimaryRecordInput{
		fileId:     pi.tree.MetaPageId().FileId(),
		pkCount:    pi.pkCount,
		deleteMark: 0,
		rollPtr:    undo.NullPointer(),
		colNames:   []string{"id", "name", "email"},
		values:     []string{id, name, email},
	})
	if err != nil {
		t.Fatalf("PrimaryRecord の構築に失敗: %v", err)
	}
	return pr
}

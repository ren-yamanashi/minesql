package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
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
		created, err := createPrimaryIndex(env.ct, env.bp, page.FileId(2), 1, lockMgr)
		assert.NoError(t, err)

		// WHEN
		pi := newPrimaryIndex(env.ct, env.bp, created.tree.MetaPageId, 1, lockMgr)

		// THEN
		assert.NotNil(t, pi)
	})
}

func TestCreatePrimaryIndex(t *testing.T) {
	t.Run("プライマリインデックスを新規作成できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		lockMgr := lock.NewManager()

		// WHEN
		pi, err := createPrimaryIndex(env.ct, env.bp, page.FileId(2), 1, lockMgr)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, pi)
	})
}

func TestPrimaryIndexInsert(t *testing.T) {
	t.Run("レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		record := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")

		// WHEN
		err := pi.insert(record, testTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一プライマリキーの重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r1 := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.insert(r1, testTrxId)
		r2 := buildTestPrimaryRecord(t, pi, "1", "Bob", "b@example.com")

		// WHEN
		err := pi.insert(r2, testTrxId)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("異なるプライマリキーであれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r1 := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.insert(r1, testTrxId)
		r2 := buildTestPrimaryRecord(t, pi, "2", "Bob", "b@example.com")

		// WHEN
		err := pi.insert(r2, testTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("論理削除済みの同一キーがある場合は上書きできる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r1 := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.insert(r1, testTrxId)

		// 論理削除
		iter, _ := pi.search(SearchModeStart{})
		record, _, _ := iter.next()
		_ = pi.softDelete(record, testTrxId)

		r2 := buildTestPrimaryRecord(t, pi, "1", "Bob", "b@example.com")

		// WHEN
		err := pi.insert(r2, testTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("挿入後に排他ロックが取得される", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		record := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")

		// WHEN
		err := pi.insert(record, testTrxId)

		// THEN
		assert.NoError(t, err)
		// 同一トランザクションで既に排他ロックを保持しているので再取得は成功する
		encodedRecord := record.encode()
		_, pos, err := pi.tree.FindByKey(encodedRecord.Key())
		assert.NoError(t, err)
		err = pi.lock.Lock(testTrxId, pos, lock.Exclusive)
		assert.NoError(t, err)
	})
}

func TestPrimaryIndexSearch(t *testing.T) {
	t.Run("全件スキャンでレコードを取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		record := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(record, testTrxId)

		// WHEN
		iter, err := pi.search(SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		result, ok, err := iter.next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, result.Values)
	})

	t.Run("空のインデックスを検索するとデータなしを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		iter, err := pi.search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestPrimaryIndexSoftDelete(t *testing.T) {
	t.Run("レコードを論理削除できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(r, testTrxId)

		iter, _ := pi.search(SearchModeStart{})
		record, _, _ := iter.next()

		// WHEN
		err := pi.softDelete(record, testTrxId)

		// THEN
		assert.NoError(t, err)

		// 論理削除後は検索でスキップされる
		iter2, _ := pi.search(SearchModeStart{})
		_, ok, _ := iter2.next()
		assert.False(t, ok)
	})

	t.Run("論理削除後に再挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(r, testTrxId)

		iter, _ := pi.search(SearchModeStart{})
		record, _, _ := iter.next()
		_ = pi.softDelete(record, testTrxId)

		r2 := buildTestPrimaryRecord(t, pi, "1", "Bob", "bob@example.com")

		// WHEN
		err := pi.insert(r2, testTrxId)

		// THEN
		assert.NoError(t, err)
	})
}

func TestPrimaryIndexUpdate(t *testing.T) {
	t.Run("レコードをインプレース更新できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(r, testTrxId)

		iter, _ := pi.search(SearchModeStart{})
		current, _, _ := iter.next()
		newRecord, _ := current.update(testTrxId, []string{"name"}, []string{"Bob"})

		// WHEN
		err := pi.update(current, newRecord, testTrxId)

		// THEN
		assert.NoError(t, err)

		// 更新後の値を確認
		iter2, _ := pi.search(SearchModeStart{})
		updated, ok, _ := iter2.next()
		assert.True(t, ok)
		assert.Equal(t, "Bob", updated.Values[1])
		assert.Equal(t, "alice@example.com", updated.Values[2])
	})

	t.Run("存在しないカラムで更新するとエラーを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.insert(r, testTrxId)

		iter, _ := pi.search(SearchModeStart{})
		current, _, _ := iter.next()

		// WHEN
		_, err := current.update(testTrxId, []string{"nonexistent"}, []string{"val"})

		// THEN
		assert.Error(t, err)
	})
}

func TestPrimaryIndexDelete(t *testing.T) {
	t.Run("レコードを物理削除できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.insert(r, testTrxId)

		iter, _ := pi.search(SearchModeStart{})
		record, _, _ := iter.next()

		// WHEN
		err := pi.delete(record, testTrxId)

		// THEN
		assert.NoError(t, err)

		// 削除後は取得できない
		iter2, _ := pi.search(SearchModeStart{})
		_, ok, _ := iter2.next()
		assert.False(t, ok)
	})

	t.Run("存在しないレコードの削除はエラーを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		pr := buildTestPrimaryRecord(t, pi, "999", "Nobody", "no@example.com")

		// WHEN
		err := pi.delete(pr, testTrxId)

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

		// WHEN
		count, err := pi.leafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestPrimaryIndexHeight(t *testing.T) {
	t.Run("ツリーの高さを取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		height, err := pi.height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// setupTestPrimaryIndex はテスト用の PrimaryIndex (pkCount=1) を作成する
func setupTestPrimaryIndex(t *testing.T) *primaryIndex {
	t.Helper()
	env := setupIteratorTestEnv(t)
	lockMgr := lock.NewManager()
	pi, err := createPrimaryIndex(env.ct, env.bp, page.FileId(2), 1, lockMgr)
	if err != nil {
		t.Fatalf("PrimaryIndex の作成に失敗: %v", err)
	}
	return pi
}

// buildTestPrimaryRecord はテスト用の PrimaryRecord (id, name, email) を構築する
func buildTestPrimaryRecord(t *testing.T, pi *primaryIndex, id, name, email string) *primaryRecord {
	t.Helper()
	pr, err := newPrimaryRecord(pi.catalog, newPrimaryRecordInput{
		fileId:     pi.tree.MetaPageId.FileId,
		pkCount:    pi.pkCount,
		deleteMark: 0,
		rollPtr:    undo.NullPointer,
		colNames:   []string{"id", "name", "email"},
		values:     []string{id, name, email},
	})
	if err != nil {
		t.Fatalf("PrimaryRecord の構築に失敗: %v", err)
	}
	return pr
}

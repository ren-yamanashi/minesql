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
		created, err := CreatePrimaryIndex(env.ct, env.bp, page.FileId(2), 1, lockMgr)
		assert.NoError(t, err)

		// WHEN
		pi := NewPrimaryIndex(env.ct, env.bp, created.tree.MetaPageId, 1, lockMgr)

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
		pi, err := CreatePrimaryIndex(env.ct, env.bp, page.FileId(2), 1, lockMgr)

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
		err := pi.Insert(record, testTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一プライマリキーの重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r1 := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.Insert(r1, testTrxId)
		r2 := buildTestPrimaryRecord(t, pi, "1", "Bob", "b@example.com")

		// WHEN
		err := pi.Insert(r2, testTrxId)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("異なるプライマリキーであれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r1 := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.Insert(r1, testTrxId)
		r2 := buildTestPrimaryRecord(t, pi, "2", "Bob", "b@example.com")

		// WHEN
		err := pi.Insert(r2, testTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("論理削除済みの同一キーがある場合は上書きできる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r1 := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.Insert(r1, testTrxId)

		// 論理削除
		iter, _ := pi.Search(SearchModeStart{})
		record, _, _ := iter.Next()
		_ = pi.SoftDelete(record, testTrxId)

		r2 := buildTestPrimaryRecord(t, pi, "1", "Bob", "b@example.com")

		// WHEN
		err := pi.Insert(r2, testTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("挿入後に排他ロックが取得される", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		record := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")

		// WHEN
		err := pi.Insert(record, testTrxId)

		// THEN
		assert.NoError(t, err)
		// 同一トランザクションで既に排他ロックを保持しているので再取得は成功する
		encodedRecord := record.Encode()
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
		_ = pi.Insert(record, testTrxId)

		// WHEN
		iter, err := pi.Search(SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		result, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, result.Values)
	})

	t.Run("空のインデックスを検索するとデータなしを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		iter, err := pi.Search(SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

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
		_ = pi.Insert(r, testTrxId)

		iter, _ := pi.Search(SearchModeStart{})
		record, _, _ := iter.Next()

		// WHEN
		err := pi.SoftDelete(record, testTrxId)

		// THEN
		assert.NoError(t, err)

		// 論理削除後は検索でスキップされる
		iter2, _ := pi.Search(SearchModeStart{})
		_, ok, _ := iter2.Next()
		assert.False(t, ok)
	})

	t.Run("論理削除後に再挿入できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.Insert(r, testTrxId)

		iter, _ := pi.Search(SearchModeStart{})
		record, _, _ := iter.Next()
		_ = pi.SoftDelete(record, testTrxId)

		r2 := buildTestPrimaryRecord(t, pi, "1", "Bob", "bob@example.com")

		// WHEN
		err := pi.Insert(r2, testTrxId)

		// THEN
		assert.NoError(t, err)
	})
}

func TestPrimaryIndexUpdate(t *testing.T) {
	t.Run("レコードをインプレース更新できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "alice@example.com")
		_ = pi.Insert(r, testTrxId)

		iter, _ := pi.Search(SearchModeStart{})
		current, _, _ := iter.Next()
		newRecord, _ := current.update(testTrxId, []string{"name"}, []string{"Bob"})

		// WHEN
		err := pi.Update(current, newRecord, testTrxId)

		// THEN
		assert.NoError(t, err)

		// 更新後の値を確認
		iter2, _ := pi.Search(SearchModeStart{})
		updated, ok, _ := iter2.Next()
		assert.True(t, ok)
		assert.Equal(t, "Bob", updated.Values[1])
		assert.Equal(t, "alice@example.com", updated.Values[2])
	})

	t.Run("存在しないカラムで更新するとエラーを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		r := buildTestPrimaryRecord(t, pi, "1", "Alice", "a@example.com")
		_ = pi.Insert(r, testTrxId)

		iter, _ := pi.Search(SearchModeStart{})
		current, _, _ := iter.Next()

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
		_ = pi.Insert(r, testTrxId)

		iter, _ := pi.Search(SearchModeStart{})
		record, _, _ := iter.Next()

		// WHEN
		err := pi.Delete(record, testTrxId)

		// THEN
		assert.NoError(t, err)

		// 削除後は取得できない
		iter2, _ := pi.Search(SearchModeStart{})
		_, ok, _ := iter2.Next()
		assert.False(t, ok)
	})

	t.Run("存在しないレコードの削除はエラーを返す", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)
		pr := buildTestPrimaryRecord(t, pi, "999", "Nobody", "no@example.com")

		// WHEN
		err := pi.Delete(pr, testTrxId)

		// THEN
		assert.Error(t, err)
	})
}

func TestPrimaryIndexLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		pi := setupTestPrimaryIndex(t)

		// WHEN
		count, err := pi.LeafPageCount()

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
		height, err := pi.Height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

// setupTestPrimaryIndex はテスト用の PrimaryIndex (pkCount=1) を作成する
func setupTestPrimaryIndex(t *testing.T) *PrimaryIndex {
	t.Helper()
	env := setupIteratorTestEnv(t)
	lockMgr := lock.NewManager()
	pi, err := CreatePrimaryIndex(env.ct, env.bp, page.FileId(2), 1, lockMgr)
	if err != nil {
		t.Fatalf("PrimaryIndex の作成に失敗: %v", err)
	}
	return pi
}

// buildTestPrimaryRecord はテスト用の PrimaryRecord (id, name, email) を構築する
func buildTestPrimaryRecord(t *testing.T, pi *PrimaryIndex, id, name, email string) *PrimaryRecord {
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

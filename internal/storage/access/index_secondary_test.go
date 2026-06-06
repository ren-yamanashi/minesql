package access

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

const testSecondaryTrxId lock.TrxId = 1

func TestNewSecondaryIndex(t *testing.T) {
	t.Run("既存のセカンダリインデックスを開ける", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		lockMgr := lock.NewManager()
		created, err := createSecondaryIndex(env.ct, env.bp, createSecondaryIndexInput{
			FileId:      page.FileId(2),
			PrimaryTree: env.primaryTree,
			IndexName:   "idx_name",
			Unique:      false,
			Lock:        lockMgr,
		})
		assert.NoError(t, err)

		// WHEN
		si := newSecondaryIndex(env.ct, env.bp, newSecondaryIndexInput{
			MetaPageId:  created.tree.MetaPageId(),
			PrimaryTree: env.primaryTree,
			IndexName:   "idx_name",
			Unique:      false,
			Lock:        lockMgr,
		})

		// THEN
		assert.NotNil(t, si)
	})
}

func TestCreateSecondaryIndex(t *testing.T) {
	t.Run("セカンダリインデックスを新規作成できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		lockMgr := lock.NewManager()

		// WHEN
		si, err := createSecondaryIndex(env.ct, env.bp, createSecondaryIndexInput{
			FileId:      page.FileId(2),
			PrimaryTree: env.primaryTree,
			IndexName:   "idx_name",
			Unique:      false,
			Lock:        lockMgr,
		})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, si)
	})
}

func TestSecondaryIndexSearch(t *testing.T) {
	t.Run("全件スキャンでレコードを取得できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})
		_ = si.insert(mtr, r, testSecondaryTrxId)

		// WHEN
		iter, err := si.search(mtr, SearchModeStart{}, nil)

		// THEN
		assert.NoError(t, err)
		result, ok, err := iter.NextIndexOnly()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"Alice"}, result.values)
	})
}

func TestSecondaryIndexInsert(t *testing.T) {
	t.Run("セカンダリインデックスにレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		record := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})

		// WHEN
		err := si.insert(mtr, record, testSecondaryTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同一キー (SK+PK) の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r1 := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})
		_ = si.insert(mtr, r1, testSecondaryTrxId)
		r2 := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})

		// WHEN
		err := si.insert(mtr, r2, testSecondaryTrxId)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("非ユニークインデックスでは異なる PK で同じ SK を挿入できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r1 := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})
		_ = si.insert(mtr, r1, testSecondaryTrxId)
		r2 := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"2"})

		// WHEN
		err := si.insert(mtr, r2, testSecondaryTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("ユニークインデックスでは同じ SK の挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_email", true)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r1 := buildTestSecondaryRecord(t, si, []string{"email"}, []string{"alice@example.com"}, []string{"1"})
		_ = si.insert(mtr, r1, testSecondaryTrxId)
		r2 := buildTestSecondaryRecord(t, si, []string{"email"}, []string{"alice@example.com"}, []string{"2"})

		// WHEN
		err := si.insert(mtr, r2, testSecondaryTrxId)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("ユニークインデックスで論理削除済みの SK と同じ値は挿入できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_email", true)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r1 := buildTestSecondaryRecord(t, si, []string{"email"}, []string{"alice@example.com"}, []string{"1"})
		err := si.insert(mtr, r1, testSecondaryTrxId)
		assert.NoError(t, err)

		// 論理削除
		iter, err := si.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		record, ok, err := iter.NextIndexOnly()
		assert.NoError(t, err)
		assert.True(t, ok)
		err = si.softDelete(mtr, record, testSecondaryTrxId)
		assert.NoError(t, err)

		r2 := buildTestSecondaryRecord(t, si, []string{"email"}, []string{"alice@example.com"}, []string{"2"})

		// WHEN
		err = si.insert(mtr, r2, testSecondaryTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("論理削除済みの同一キー (SK+PK) がある場合は上書きできる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r1 := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})
		err := si.insert(mtr, r1, testSecondaryTrxId)
		assert.NoError(t, err)

		// 論理削除
		iter, err := si.search(mtr, SearchModeStart{}, nil)
		assert.NoError(t, err)
		record, ok, err := iter.NextIndexOnly()
		assert.NoError(t, err)
		assert.True(t, ok)
		err = si.softDelete(mtr, record, testSecondaryTrxId)
		assert.NoError(t, err)

		r2 := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})

		// WHEN
		err = si.insert(mtr, r2, testSecondaryTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("挿入後に排他ロックが取得される", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		record := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})

		// WHEN
		err := si.insert(mtr, record, testSecondaryTrxId)

		// THEN
		assert.NoError(t, err)
		// 別トランザクションが同じレコードに排他ロックを取得しようとするとタイムアウト
		encodedRecord := record.Encode()
		rowKey := lock.RowKey{MetaPageId: si.tree.MetaPageId(), Key: encodedRecord.Key()}
		err = si.lock.Lock(lock.TrxId(999), rowKey, lock.Exclusive)
		assert.ErrorIs(t, err, lock.ErrTimeout)
	})
}

func TestSecondaryIndexDelete(t *testing.T) {
	t.Run("レコードを物理削除できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})
		_ = si.insert(mtr, r, testSecondaryTrxId)

		iter, _ := si.search(mtr, SearchModeStart{}, nil)
		record, _, _ := iter.NextIndexOnly()

		// WHEN
		err := si.delete(mtr, record, testSecondaryTrxId)

		// THEN
		assert.NoError(t, err)

		// 削除後は取得できない
		iter2, _ := si.search(mtr, SearchModeStart{}, nil)
		_, ok, _ := iter2.NextIndexOnly()
		assert.False(t, ok)
	})

	t.Run("存在しないレコードの削除はエラーを返す", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		sr := &SecondaryRecord{
			colNames: []string{"name"},
			values:   []string{"nonexistent"},
			pk:       []string{"999"},
		}

		// WHEN
		err := si.delete(mtr, sr, testSecondaryTrxId)

		// THEN
		assert.Error(t, err)
	})
}

func TestSecondaryIndexSoftDelete(t *testing.T) {
	t.Run("レコードを論理削除できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})
		_ = si.insert(mtr, r, testSecondaryTrxId)

		iter, _ := si.search(mtr, SearchModeStart{}, nil)
		record, _, _ := iter.NextIndexOnly()

		// WHEN
		err := si.softDelete(mtr, record, testSecondaryTrxId)

		// THEN
		assert.NoError(t, err)

		// 論理削除後は検索でスキップされる
		iter2, _ := si.search(mtr, SearchModeStart{}, nil)
		_, ok, _ := iter2.NextIndexOnly()
		assert.False(t, ok)
	})

	t.Run("論理削除後に再挿入できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})
		_ = si.insert(mtr, r, testSecondaryTrxId)

		iter, _ := si.search(mtr, SearchModeStart{}, nil)
		record, _, _ := iter.NextIndexOnly()
		_ = si.softDelete(mtr, record, testSecondaryTrxId)

		r2 := buildTestSecondaryRecord(t, si, []string{"name"}, []string{"Alice"}, []string{"1"})

		// WHEN
		err := si.insert(mtr, r2, testSecondaryTrxId)

		// THEN
		assert.NoError(t, err)
	})
}

func TestSecondaryIndexLeafPageCount(t *testing.T) {
	t.Run("リーフページ数を取得できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)

		// WHEN
		count, err := si.leafPageCount()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count)
	})
}

func TestSecondaryIndexHeight(t *testing.T) {
	t.Run("ツリーの高さを取得できる", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_name", false)

		// WHEN
		height, err := si.height()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), height)
	})
}

func TestSecondaryIndexCheckUnique(t *testing.T) {
	t.Run("重複がない場合はエラーを返さない", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_email", true)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		sr := buildTestSecondaryRecord(t, si, []string{"email"}, []string{"alice@example.com"}, []string{"1"})

		// WHEN
		err := si.checkUnique(mtr, sr)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じ SK のレコードが存在する場合は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_email", true)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r1 := buildTestSecondaryRecord(t, si, []string{"email"}, []string{"alice@example.com"}, []string{"1"})
		_ = si.insert(mtr, r1, testSecondaryTrxId)
		r2 := buildTestSecondaryRecord(t, si, []string{"email"}, []string{"alice@example.com"}, []string{"2"})

		// WHEN
		err := si.checkUnique(mtr, r2)

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("論理削除済みの同じ SK のレコードが存在する場合はエラーを返さない", func(t *testing.T) {
		// GIVEN
		si := setupTestSecondaryIndex(t, "idx_email", true)
		mtr := buffer.NewMtr(si.bufferPool)
		defer mtr.UnpinAll()
		r1 := buildTestSecondaryRecord(t, si, []string{"email"}, []string{"alice@example.com"}, []string{"1"})
		_ = si.insert(mtr, r1, testSecondaryTrxId)

		// 論理削除
		iter, _ := si.search(mtr, SearchModeStart{}, nil)
		record, _, _ := iter.NextIndexOnly()
		_ = si.softDelete(mtr, record, testSecondaryTrxId)

		r2 := buildTestSecondaryRecord(t, si, []string{"email"}, []string{"alice@example.com"}, []string{"2"})

		// WHEN
		err := si.checkUnique(mtr, r2)

		// THEN
		assert.NoError(t, err)
	})
}

// setupTestSecondaryIndex はテスト用の SecondaryIndex を作成する
func setupTestSecondaryIndex(t *testing.T, indexName string, unique bool) *secondaryIndex {
	t.Helper()
	env := setupIteratorTestEnv(t)
	lockMgr := lock.NewManager()
	si, err := createSecondaryIndex(env.ct, env.bp, createSecondaryIndexInput{
		FileId:      page.FileId(2),
		PrimaryTree: env.primaryTree,
		IndexName:   indexName,
		Unique:      unique,
		Lock:        lockMgr,
	})
	if err != nil {
		t.Fatalf("SecondaryIndex の作成に失敗: %v", err)
	}
	return si
}

// buildTestSecondaryRecord はテスト用の SecondaryRecord を構築する
func buildTestSecondaryRecord(t *testing.T, si *secondaryIndex, colNames, values, pk []string) *SecondaryRecord {
	t.Helper()
	sr, err := NewSecondaryRecord(si.catalog, si.bufferPool, NewSecondaryRecordInput{
		fileId:     si.fileId,
		deleteMark: 0,
		indexName:  si.indexName,
		colNames:   colNames,
		values:     values,
		pk:         pk,
	})
	if err != nil {
		t.Fatalf("SecondaryRecord の構築に失敗: %v", err)
	}
	return sr
}

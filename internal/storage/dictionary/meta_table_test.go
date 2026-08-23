package dictionary

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestCreateTableMeta(t *testing.T) {
	t.Run("テーブルメタデータを新規作成できる", func(t *testing.T) {
		// GIVEN
		bp := setupDictTestBufferPool(t)
		rl := setupDictTestRedoBuffer(t)

		// WHEN
		ctMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, rl)
		tm := CreateTableMeta(ctMtr)
		_ = ctMtr.Commit()

		// THEN
		assert.False(t, tm.tree.MetaPageId().IsInvalid())
	})
}

func TestTableMetaSearch(t *testing.T) {
	t.Run("SearchModeStart で全件スキャンできる", func(t *testing.T) {
		// GIVEN
		tm, bp := setupTestTableMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = tm.Insert(mtr, NewTableMetaRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3))
		_ = tm.Insert(mtr, NewTableMetaRecord("orders", page.NewId(page.FileId(2), page.PageNumber(0)), 5))

		// WHEN
		iter, err := tm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN: テーブル名でソートされる
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "orders", r1.Name())
		assert.Equal(t, page.NewId(page.FileId(2), page.PageNumber(0)), r1.MetaPageId())
		assert.Equal(t, 5, r1.ColumnCount())

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "users", r2.Name())
		assert.Equal(t, page.NewId(page.FileId(1), page.PageNumber(0)), r2.MetaPageId())
		assert.Equal(t, 3, r2.ColumnCount())

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("空のメタデータを検索するとレコードが返らない", func(t *testing.T) {
		// GIVEN
		tm, bp := setupTestTableMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()

		// WHEN
		iter, err := tm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestTableMetaInsert(t *testing.T) {
	t.Run("テーブルメタデータを挿入できる", func(t *testing.T) {
		// GIVEN
		tm, bp := setupTestTableMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()

		// WHEN
		err := tm.Insert(mtr, NewTableMetaRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3))

		// THEN
		assert.NoError(t, err)
	})

	t.Run("同じテーブル名の重複挿入は ErrDuplicateKey を返す", func(t *testing.T) {
		// GIVEN
		tm, bp := setupTestTableMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = tm.Insert(mtr, NewTableMetaRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3))

		// WHEN
		err := tm.Insert(mtr, NewTableMetaRecord("users", page.NewId(page.FileId(2), page.PageNumber(0)), 5))

		// THEN
		assert.ErrorIs(t, err, btree.ErrDuplicateKey)
	})

	t.Run("異なるテーブル名であれば複数挿入できる", func(t *testing.T) {
		// GIVEN
		tm, bp := setupTestTableMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		_ = tm.Insert(mtr, NewTableMetaRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3))

		// WHEN
		err := tm.Insert(mtr, NewTableMetaRecord("orders", page.NewId(page.FileId(2), page.PageNumber(0)), 5))

		// THEN
		assert.NoError(t, err)
	})
}

func TestTableMetaDelete(t *testing.T) {
	t.Run("Insert したレコードを Delete で削除できる", func(t *testing.T) {
		// GIVEN
		tm, bp := setupTestTableMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		record := NewTableMetaRecord("users", page.NewId(page.FileId(1), page.PageNumber(0)), 3)
		_ = tm.Insert(mtr, record)

		// WHEN
		err := tm.Delete(mtr, record.Encode().Key())

		// THEN
		assert.NoError(t, err)
		iter, err := tm.Search(mtr, SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("存在しないキーの Delete は ErrKeyNotFound を返す", func(t *testing.T) {
		// GIVEN
		tm, bp := setupTestTableMeta(t)
		mtr := buffer.NewMtr(bp)
		defer mtr.UnpinAll()
		missing := NewTableMetaRecord("missing", page.NewId(page.FileId(1), page.PageNumber(0)), 1)

		// WHEN
		err := tm.Delete(mtr, missing.Encode().Key())

		// THEN
		assert.ErrorIs(t, err, btree.ErrKeyNotFound)
	})
}

// setupTestTableMeta はテスト用の TableMeta を作成する
func setupTestTableMeta(t *testing.T) (*TableMeta, *buffer.Pool) {
	t.Helper()
	bp := setupDictTestBufferPool(t)
	ctMtr := buffer.NewWriteMtr(bp, lock.SystemReservedTrxId, setupDictTestRedoBuffer(t))
	tm := CreateTableMeta(ctMtr)
	if err := ctMtr.Commit(); err != nil {
		t.Fatalf("TableMeta セットアップ Mtr の Commit に失敗: %v", err)
	}
	return tm, bp
}

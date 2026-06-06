package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/dictionary"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestSecondaryIndexIteratorNext(t *testing.T) {
	t.Run("セカンダリインデックス経由でプライマリレコードを取得できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"1", "Alice", "alice@example.com"})
		insertSecondaryRecord(t, env, []string{"name"}, []string{"Alice"}, []string{"1"})

		iter := searchSecondaryIndex(t, env)

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, result.values)
	})

	t.Run("複数レコードを順に取得できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"2", "Bob", "b@example.com"})
		insertSecondaryRecord(t, env, []string{"name"}, []string{"Alice"}, []string{"1"})
		insertSecondaryRecord(t, env, []string{"name"}, []string{"Bob"}, []string{"2"})

		iter := searchSecondaryIndex(t, env)

		// WHEN
		r1, ok1, err1 := iter.Next()
		r2, ok2, err2 := iter.Next()
		_, ok3, err3 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "Alice", r1.values[1])

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "Bob", r2.values[1])

		assert.NoError(t, err3)
		assert.False(t, ok3)
	})

	t.Run("論理削除されたレコードをスキップする", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"1", "Alice", "a@example.com"})
		insertPrimaryRecord(t, env, 0, []string{"id", "name", "email"}, []string{"2", "Bob", "b@example.com"})
		insertSecondaryRecord(t, env, []string{"name"}, []string{"Alice"}, []string{"1"})
		insertSecondaryRecordWithDeleteMark(t, env, 1, []string{"name"}, []string{"Bob"}, []string{"2"})

		iter := searchSecondaryIndex(t, env)

		// WHEN
		r1, ok1, err1 := iter.Next()
		_, ok2, err2 := iter.Next()

		// THEN
		assert.NoError(t, err1)
		assert.True(t, ok1)
		assert.Equal(t, "Alice", r1.values[1])

		assert.NoError(t, err2)
		assert.False(t, ok2)
	})

	t.Run("プライマリに対応するレコードが存在しない場合データなしを返す", func(t *testing.T) {
		// GIVEN: セカンダリにはあるがプライマリにはない PK
		env := setupIteratorTestEnv(t)
		insertSecondaryRecord(t, env, []string{"name"}, []string{"Alice"}, []string{"999"})

		iter := searchSecondaryIndex(t, env)

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("空のインデックスから取得するとデータなしを返す", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		iter := searchSecondaryIndex(t, env)

		// WHEN
		_, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestSecondaryIndexIteratorNextIndexOnly(t *testing.T) {
	t.Run("セカンダリインデックスのレコードのみを取得できる", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertSecondaryRecord(t, env, []string{"name"}, []string{"Alice"}, []string{"1"})

		iter := searchSecondaryIndex(t, env)

		// WHEN
		result, ok, err := iter.NextIndexOnly()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"name"}, result.colNames)
		assert.Equal(t, []string{"Alice"}, result.values)
		assert.Equal(t, []string{"1"}, result.pk)
	})

	t.Run("論理削除されたレコードをスキップする", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		insertSecondaryRecordWithDeleteMark(t, env, 1, []string{"name"}, []string{"Alice"}, []string{"1"})
		insertSecondaryRecord(t, env, []string{"name"}, []string{"Bob"}, []string{"2"})

		iter := searchSecondaryIndex(t, env)

		// WHEN
		result, ok, err := iter.NextIndexOnly()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"Bob"}, result.values)
	})

	t.Run("空のインデックスから取得するとデータなしを返す", func(t *testing.T) {
		// GIVEN
		env := setupIteratorTestEnv(t)
		iter := searchSecondaryIndex(t, env)

		// WHEN
		_, ok, err := iter.NextIndexOnly()

		// THEN
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

// iteratorTestEnv はイテレータテスト用の環境
type iteratorTestEnv struct {
	ct            *dictionary.Catalog
	bp            *buffer.Pool
	primaryTree   *btree.Tree
	secondaryTree *btree.Tree
}

// setupIteratorTestEnv はセカンダリイテレータのテスト用環境を構築する
func setupIteratorTestEnv(t *testing.T) *iteratorTestEnv {
	t.Helper()

	// カタログ用 HeapFile (FileId=0)
	catalogPath := filepath.Join(t.TempDir(), "catalog.db")
	catalogHf, err := file.NewHeapFile(page.FileId(0), catalogPath)
	if err != nil {
		t.Fatalf("カタログ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = catalogHf.Close() })

	// テーブルデータ用 HeapFile (FileId=2)
	dataPath := filepath.Join(t.TempDir(), "data.db")
	dataHf, err := file.NewHeapFile(page.FileId(2), dataPath)
	if err != nil {
		t.Fatalf("データ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = dataHf.Close() })

	bp := buffer.NewPool(page.Size*50, nil)
	bp.RegisterHeapFile(page.FileId(0), catalogHf)
	bp.RegisterHeapFile(page.FileId(2), dataHf)

	ct, err := dictionary.CreateCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の作成に失敗: %v", err)
	}

	// テーブル定義: id:0, name:1, email:2
	tableFileId := page.FileId(2)
	dummyPageId := page.NewId(tableFileId, page.PageNumber(0))
	mtr := buffer.NewMtr(bp)
	defer mtr.UnpinAll()
	_ = ct.TableMeta().Insert(mtr, dictionary.NewTableMetaRecord("users", dummyPageId, 3))
	_ = ct.ColumnMeta().Insert(mtr, dictionary.NewColumnMetaRecord(tableFileId, "id", 0))
	_ = ct.ColumnMeta().Insert(mtr, dictionary.NewColumnMetaRecord(tableFileId, "name", 1))
	_ = ct.ColumnMeta().Insert(mtr, dictionary.NewColumnMetaRecord(tableFileId, "email", 2))

	// インデックス定義
	indexId1 := dictionary.IndexId(1)
	_ = ct.IndexMeta().Insert(mtr, dictionary.NewIndexMetaRecord(tableFileId, indexId1, "idx_name", dictionary.IndexTypeNonUnique, 1, dummyPageId))
	_ = ct.IndexKeyColumnMeta().Insert(mtr, dictionary.NewIndexKeyColumnMetaRecord(indexId1, "name", 0))

	indexId2 := dictionary.IndexId(2)
	_ = ct.IndexMeta().Insert(mtr, dictionary.NewIndexMetaRecord(tableFileId, indexId2, "idx_email", dictionary.IndexTypeUnique, 1, dummyPageId))
	_ = ct.IndexKeyColumnMeta().Insert(mtr, dictionary.NewIndexKeyColumnMetaRecord(indexId2, "email", 0))

	// プライマリ B+Tree
	primaryTree, err := btree.CreateTree(bp, tableFileId)
	if err != nil {
		t.Fatalf("プライマリ B+Tree の作成に失敗: %v", err)
	}

	// セカンダリ B+Tree
	secondaryTree, err := btree.CreateTree(bp, tableFileId)
	if err != nil {
		t.Fatalf("セカンダリ B+Tree の作成に失敗: %v", err)
	}

	return &iteratorTestEnv{
		ct:            ct,
		bp:            bp,
		primaryTree:   primaryTree,
		secondaryTree: secondaryTree,
	}
}

// insertPrimaryRecord はプライマリ B+Tree にレコードを挿入する (pkCount=1)
func insertPrimaryRecord(t *testing.T, env *iteratorTestEnv, deleteMark byte, colNames, values []string) {
	t.Helper()
	pr, err := NewPrimaryRecord(env.ct, env.bp, NewPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: deleteMark, colNames: colNames, values: values})
	if err != nil {
		t.Fatalf("PrimaryRecord の作成に失敗: %v", err)
	}
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	if err := env.primaryTree.Insert(mtr, pr.Encode()); err != nil {
		t.Fatalf("プライマリレコードの挿入に失敗: %v", err)
	}
}

// insertSecondaryRecord はセカンダリ B+Tree に idx_name インデックスのレコードを挿入する (deleteMark=0)
func insertSecondaryRecord(t *testing.T, env *iteratorTestEnv, colNames, values, pk []string) {
	t.Helper()
	insertSecondaryRecordWithDeleteMark(t, env, 0, colNames, values, pk)
}

// insertSecondaryRecordWithDeleteMark はセカンダリ B+Tree に指定した deleteMark でレコードを挿入する
func insertSecondaryRecordWithDeleteMark(t *testing.T, env *iteratorTestEnv, deleteMark byte, colNames, values, pk []string) {
	t.Helper()
	sr, err := NewSecondaryRecord(env.ct, env.bp, NewSecondaryRecordInput{
		fileId:     page.FileId(2),
		deleteMark: deleteMark,
		indexName:  "idx_name",
		colNames:   colNames,
		values:     values,
		pk:         pk,
	})
	if err != nil {
		t.Fatalf("SecondaryRecord の作成に失敗: %v", err)
	}
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	if err := env.secondaryTree.Insert(mtr, sr.Encode()); err != nil {
		t.Fatalf("セカンダリレコードの挿入に失敗: %v", err)
	}
}

// searchSecondaryIndex はセカンダリ B+Tree を先頭から検索してイテレータを返す
func searchSecondaryIndex(t *testing.T, env *iteratorTestEnv) *SecondaryIndexIterator {
	t.Helper()
	mode := SearchModeStart{}
	mtr := buffer.NewMtr(env.bp)
	defer mtr.UnpinAll()
	iter, err := env.secondaryTree.Search(mtr, mode.Encode())
	if err != nil {
		t.Fatalf("セカンダリインデックスの検索に失敗: %v", err)
	}
	return NewSecondaryIndexIterator("idx_name", iter, env.ct, env.bp, env.primaryTree, nil, nil)
}

func TestSecondaryIndexIteratorNextWithReadView(t *testing.T) {
	t.Run("readView 経由でプライマリレコードを取得できる", func(t *testing.T) {
		// GIVEN
		env := setupMVCCTestEnv(t)
		insertPrimaryRecordWithMvcc(t, env, lock.TrxId(1), undo.NullPointer(), "1", "Alice", "a@example.com")
		insertSecondaryRecord(t, env.iter, []string{"name"}, []string{"Alice"}, []string{"1"})

		rv := newReadView(lock.TrxId(2), nil, lock.TrxId(2))
		iter := searchSecondaryIndexWithReadView(t, env, rv)
		defer iter.Close()

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
	})

	t.Run("プライマリの最新が不可視なら Undo 遡及で旧値を返す", func(t *testing.T) {
		// GIVEN
		env := setupMVCCTestEnv(t)
		insertedRecord := insertPrimaryRecordWithMvcc(t, env, lock.TrxId(1), undo.NullPointer(), "1", "Alice", "a@example.com")
		updateUndo := undo.NewUpdateRecord(page.FileId(2), insertedRecord.Encode(), btree.Record{}, lock.TrxId(1), undo.NullPointer())
		ptr := env.appendUndo(t, lock.TrxId(3), undo.RecordTypeUpdate, updateUndo)
		updatePrimaryRecordWithMvcc(t, env, lock.TrxId(3), ptr, "1", "Bob", "a@example.com")
		insertSecondaryRecord(t, env.iter, []string{"name"}, []string{"Alice"}, []string{"1"})

		rv := newReadView(lock.TrxId(2), []lock.TrxId{lock.TrxId(3)}, lock.TrxId(4))
		iter := searchSecondaryIndexWithReadView(t, env, rv)
		defer iter.Close()

		// WHEN
		result, ok, err := iter.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", result.values[1])
	})
}

// searchSecondaryIndexWithReadView は readView 付きでセカンダリイテレータを返す
func searchSecondaryIndexWithReadView(t *testing.T, env *mvccTestEnv, rv *readView) *SecondaryIndexIterator {
	t.Helper()
	mtr := buffer.NewMtr(env.iter.bp)
	t.Cleanup(func() { mtr.UnpinAll() })
	iter, err := env.iter.secondaryTree.Search(mtr, SearchModeStart{}.Encode())
	if err != nil {
		t.Fatalf("セカンダリインデックスの検索に失敗: %v", err)
	}
	return NewSecondaryIndexIterator("idx_name", iter, env.iter.ct, env.iter.bp, env.iter.primaryTree, rv, env.undoLog)
}

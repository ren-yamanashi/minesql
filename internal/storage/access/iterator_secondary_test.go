package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestSecondaryIteratorNext(t *testing.T) {
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
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, result.Values)
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
		assert.Equal(t, "Alice", r1.Values[1])

		assert.NoError(t, err2)
		assert.True(t, ok2)
		assert.Equal(t, "Bob", r2.Values[1])

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
		assert.Equal(t, "Alice", r1.Values[1])

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

func TestSecondaryIteratorNextIndexOnly(t *testing.T) {
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
		assert.Equal(t, []string{"name"}, result.ColNames)
		assert.Equal(t, []string{"Alice"}, result.Values)
		assert.Equal(t, []string{"1"}, result.Pk)
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
		assert.Equal(t, []string{"Bob"}, result.Values)
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
	ct            *catalog.Catalog
	bp            *buffer.BufferPool
	primaryTree   *btree.Btree
	secondaryTree *btree.Btree
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

	bp := buffer.NewBufferPool(page.PageSize * 50)
	bp.RegisterHeapFile(page.FileId(0), catalogHf)
	bp.RegisterHeapFile(page.FileId(2), dataHf)

	ct, err := catalog.CreateCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の作成に失敗: %v", err)
	}

	// テーブル定義: id:0, name:1, email:2
	tableFileId := page.FileId(2)
	dummyPageId := page.NewPageId(tableFileId, page.PageNumber(0))
	_ = ct.TableMeta.Insert("users", dummyPageId, 3)
	_ = ct.ColumnMeta.Insert(tableFileId, "id", 0)
	_ = ct.ColumnMeta.Insert(tableFileId, "name", 1)
	_ = ct.ColumnMeta.Insert(tableFileId, "email", 2)

	// インデックス定義
	indexId1 := catalog.IndexId(1)
	_ = ct.IndexMeta.Insert(tableFileId, "idx_name", indexId1, catalog.IndexTypeNonUnique, 1, dummyPageId)
	_ = ct.IndexKeyColMeta.Insert(indexId1, "name", 0)

	indexId2 := catalog.IndexId(2)
	_ = ct.IndexMeta.Insert(tableFileId, "idx_email", indexId2, catalog.IndexTypeUnique, 1, dummyPageId)
	_ = ct.IndexKeyColMeta.Insert(indexId2, "email", 0)

	// プライマリ B+Tree
	primaryTree, err := btree.CreateBtree(bp, tableFileId)
	if err != nil {
		t.Fatalf("プライマリ B+Tree の作成に失敗: %v", err)
	}

	// セカンダリ B+Tree
	secondaryTree, err := btree.CreateBtree(bp, tableFileId)
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
	pr, err := newPrimaryRecord(env.ct, newPrimaryRecordInput{fileId: page.FileId(2), pkCount: 1, deleteMark: deleteMark, colNames: colNames, values: values})
	if err != nil {
		t.Fatalf("PrimaryRecord の作成に失敗: %v", err)
	}
	if err := env.primaryTree.Insert(pr.encode()); err != nil {
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
	sr, err := newSecondaryRecord(env.ct, newSecondaryRecordInput{
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
	if err := env.secondaryTree.Insert(sr.encode()); err != nil {
		t.Fatalf("セカンダリレコードの挿入に失敗: %v", err)
	}
}

// searchSecondaryIndex はセカンダリ B+Tree を先頭から検索してイテレータを返す
func searchSecondaryIndex(t *testing.T, env *iteratorTestEnv) *SecondaryIterator {
	t.Helper()
	mode := SearchModeStart{}
	iter, err := env.secondaryTree.Search(mode.encode())
	if err != nil {
		t.Fatalf("セカンダリインデックスの検索に失敗: %v", err)
	}
	return newSecondaryIterator("idx_name", iter, env.ct, env.primaryTree)
}

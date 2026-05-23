package access

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	t.Run("カタログからテーブルを開ける", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)

		// WHEN
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, table)
		assert.NotNil(t, table.primaryIndex)
	})

	t.Run("セカンダリインデックスも復元される", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)

		// WHEN
		table, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")

		// THEN
		assert.NoError(t, err)
		assert.Len(t, table.secondaryIndexes, 2)
	})

	t.Run("存在しないテーブル名を指定するとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)

		// WHEN
		_, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, "nonexistent")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("プライマリインデックスが未登録のテーブルを開くとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnvWithoutPrimaryIndex(t)

		// WHEN
		_, err := NewTable(env.bp, env.ct, env.undoLog, env.lock, "orders")

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "primary index not found")
	})
}

func TestTableBuildValMap(t *testing.T) {
	t.Run("カラム名と値のマップを構築できる", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")

		// WHEN
		m := table.buildValMap([]string{"id", "name"}, []string{"1", "Alice"})

		// THEN
		assert.Equal(t, "1", m["id"])
		assert.Equal(t, "Alice", m["name"])
	})

	t.Run("空のスライスでは空のマップを返す", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")

		// WHEN
		m := table.buildValMap([]string{}, []string{})

		// THEN
		assert.Empty(t, m)
	})
}

func TestTableExtractPrimaryKey(t *testing.T) {
	t.Run("テーブル定義順の先頭からプライマリキーを抽出する", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")

		// WHEN
		pk := table.extractPrimaryKey([]string{"1", "Alice", "alice@example.com"})

		// THEN
		assert.Equal(t, []string{"1"}, pk)
	})
}

func TestTableExtractSecondaryKey(t *testing.T) {
	t.Run("keyCols と valMap からインデックス定義順の SK を抽出する", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
		valMap := table.buildValMap(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		keyCols := map[string]int{"name": 0}

		// WHEN
		colNames, values := table.extractSecondaryKey(keyCols, valMap)

		// THEN
		assert.Equal(t, []string{"name"}, colNames)
		assert.Equal(t, []string{"Alice"}, values)
	})

	t.Run("複数カラムのセカンダリキーを定義順で抽出する", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")
		valMap := table.buildValMap(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		keyCols := map[string]int{"email": 0, "name": 1}

		// WHEN
		colNames, values := table.extractSecondaryKey(keyCols, valMap)

		// THEN
		assert.Equal(t, []string{"email", "name"}, colNames)
		assert.Equal(t, []string{"alice@example.com", "Alice"}, values)
	})
}

func TestTableBuildSecondaryRecord(t *testing.T) {
	t.Run("セカンダリインデックス用のレコードを構築できる", func(t *testing.T) {
		// GIVEN
		env := setupTableTestEnv(t)
		table, _ := NewTable(env.bp, env.ct, env.undoLog, env.lock, "users")

		var si *secondaryIndex
		for _, s := range table.secondaryIndexes {
			if s.indexName == "idx_name" {
				si = s
				break
			}
		}
		assert.NotNil(t, si)

		// WHEN
		sr, err := table.buildSecondaryRecord(si, []string{"name"}, []string{"Alice"}, []string{"1"})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, sr)
		assert.Equal(t, []string{"Alice"}, sr.values)
		assert.Equal(t, []string{"1"}, sr.pk)
	})
}

// tableTestEnv は Table テスト用の環境
type tableTestEnv struct {
	ct      *catalog.Catalog
	bp      *buffer.Pool
	lock    *lock.Manager
	undoLog *undo.Manager
}

// setupTableTestEnv は NewTable テスト用の環境を構築する
//
// テーブル: users (id:0, name:1, email:2), PK=id, pkCount=1
// インデックス:
//   - PRIMARY: カラム (id)
//   - idx_name: NonUnique, カラム (name)
//   - idx_email: Unique, カラム (email)
func setupTableTestEnv(t *testing.T) *tableTestEnv {
	t.Helper()

	// setupIteratorTestEnv と同じバッファプール + カタログを使う
	env := setupIteratorTestEnv(t)

	// Undo 用 HeapFile (FileId=3)
	undoPath := filepath.Join(t.TempDir(), "undo.db")
	undoHf, err := file.NewHeapFile(page.FileId(3), undoPath)
	if err != nil {
		t.Fatalf("Undo HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = undoHf.Close() })
	env.bp.RegisterHeapFile(page.FileId(3), undoHf)

	undoMgr, err := undo.NewManager(env.bp, nil, page.FileId(3))
	if err != nil {
		t.Fatalf("undo.Manager の作成に失敗: %v", err)
	}

	lockMgr := lock.NewManager()

	fileId := page.FileId(2)

	// テーブルメタデータ (MetaPageId としてプライマリ B+Tree の MetaPageId を使用)
	_ = env.ct.TableMeta().Insert(catalog.NewTableRecord("users", env.primaryTree.MetaPageId(), 3))

	// プライマリインデックスメタデータ
	piIndexId := catalog.IndexId(0)
	_ = env.ct.IndexMeta().Insert(catalog.NewIndexRecord(
		fileId,
		piIndexId,
		catalog.PrimaryIndexName,
		catalog.IndexTypePrimary,
		1,
		env.primaryTree.MetaPageId(),
	))
	_ = env.ct.IndexKeyColumnMeta().Insert(catalog.NewIndexKeyColumnRecord(piIndexId, "id", 0))

	// セカンダリインデックス idx_name のメタデータ (B+Tree は secondaryTree を再利用)
	siNameId := catalog.IndexId(1)
	_ = env.ct.IndexMeta().Insert(catalog.NewIndexRecord(
		fileId,
		siNameId,
		"idx_name",
		catalog.IndexTypeNonUnique,
		1,
		env.secondaryTree.MetaPageId(),
	))
	_ = env.ct.IndexKeyColumnMeta().Insert(catalog.NewIndexKeyColumnRecord(siNameId, "name", 0))

	// セカンダリインデックス idx_email のメタデータ (新しい B+Tree が必要)
	siEmailTree, err := btree.CreateTree(env.bp, fileId)
	if err != nil {
		t.Fatalf("idx_email B+Tree の作成に失敗: %v", err)
	}
	siEmailId := catalog.IndexId(2)
	_ = env.ct.IndexMeta().Insert(catalog.NewIndexRecord(
		fileId,
		siEmailId,
		"idx_email",
		catalog.IndexTypeUnique,
		1,
		siEmailTree.MetaPageId(),
	))
	_ = env.ct.IndexKeyColumnMeta().Insert(catalog.NewIndexKeyColumnRecord(siEmailId, "email", 0))

	return &tableTestEnv{
		ct:      env.ct,
		bp:      env.bp,
		lock:    lockMgr,
		undoLog: undoMgr,
	}
}

// setupTableTestEnvWithoutPrimaryIndex はテーブルメタのみ登録し、プライマリインデックスを登録しない環境を構築する
func setupTableTestEnvWithoutPrimaryIndex(t *testing.T) *tableTestEnv {
	t.Helper()

	env := setupIteratorTestEnv(t)

	// Undo 用 HeapFile (FileId=3)
	undoPath := filepath.Join(t.TempDir(), "undo.db")
	undoHf, err := file.NewHeapFile(page.FileId(3), undoPath)
	if err != nil {
		t.Fatalf("Undo HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = undoHf.Close() })
	env.bp.RegisterHeapFile(page.FileId(3), undoHf)

	undoMgr, err := undo.NewManager(env.bp, nil, page.FileId(3))
	if err != nil {
		t.Fatalf("undo.Manager の作成に失敗: %v", err)
	}

	lockMgr := lock.NewManager()

	// テーブルメタデータのみ登録 (プライマリインデックスなし)
	_ = env.ct.TableMeta().Insert(catalog.NewTableRecord("orders", env.primaryTree.MetaPageId(), 2))

	return &tableTestEnv{
		ct:      env.ct,
		bp:      env.bp,
		lock:    lockMgr,
		undoLog: undoMgr,
	}
}

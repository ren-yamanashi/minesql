package engine

import (
	"minesql/internal/access"
	"minesql/internal/catalog"
	"minesql/internal/storage"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("グローバル Engine を初期化できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		Init()

		// THEN
		assert.NotNil(t, eng)
	})

	t.Run("複数回初期化しても同じインスタンスが返される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		engine1 := Init()
		engine2 := Init()

		// THEN
		assert.Same(t, engine1, engine2)
	})
}

func TestGet(t *testing.T) {
	t.Run("初期化後にグローバル Engine を取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()
		Init()

		// WHEN
		e := Get()

		// THEN
		assert.NotNil(t, e)
		assert.NotNil(t, e.BufferPool)
	})

	t.Run("初期化前に取得しようとすると panic", func(t *testing.T) {
		// GIVEN
		Reset()

		// WHEN & THEN
		assert.Panics(t, func() {
			Get()
		})
	})
}

func TestRegisterDmToBp(t *testing.T) {
	t.Run("Disk を BufferPool に登録できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()
		Init()
		e := Get()

		fileId := storage.FileId(1)
		tableName := "users"

		// WHEN
		err := e.RegisterDmToBp(fileId, tableName)

		// THEN
		assert.NoError(t, err)

		dm, err := e.BufferPool.GetDisk(fileId)
		assert.NoError(t, err)
		assert.NotNil(t, dm)
	})

	t.Run("同じ FileId で 2 回登録しても問題ない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()
		Init()
		e := Get()

		fileId := storage.FileId(1)
		tableName := "users"

		// WHEN
		err1 := e.RegisterDmToBp(fileId, tableName)
		err2 := e.RegisterDmToBp(fileId, tableName)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)

		dm, err := e.BufferPool.GetDisk(fileId)
		assert.NoError(t, err)
		assert.NotNil(t, dm)
	})
}

func TestInitCatalog(t *testing.T) {
	t.Run("カタログファイルが存在しない場合、新しいカタログが作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		e := Init()

		// THEN
		assert.NotNil(t, e)
		assert.NotNil(t, e.Catalog)
		assert.Equal(t, storage.FileId(1), e.Catalog.NextFileId)
	})

	t.Run("カタログファイルが既に存在する場合、既存のカタログが開かれる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// 最初の初期化でカタログを作成
		engine1 := Init()

		// FileId を採番してディスクに保存
		_, err := engine1.Catalog.AllocateFileId(engine1.BufferPool)
		assert.NoError(t, err)
		_, err = engine1.Catalog.AllocateFileId(engine1.BufferPool)
		assert.NoError(t, err)

		// ダーティーページをディスクにフラッシュ
		err = engine1.BufferPool.FlushPage()
		assert.NoError(t, err)

		// Engine をリセット
		Reset()

		// WHEN: 同じディレクトリで再初期化
		engine2 := Init()

		// THEN
		assert.NotNil(t, engine2.Catalog)
		assert.Equal(t, storage.FileId(3), engine2.Catalog.NextFileId)
	})

	t.Run("カタログの Disk が BufferPool に登録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		e := Init()

		// THEN
		dm, err := e.BufferPool.GetDisk(storage.FileId(0))
		assert.NoError(t, err)
		assert.NotNil(t, dm)
	})

	t.Run("既存のテーブルがある場合、再初期化でテーブルの Disk が登録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()

		// テーブルを作成してカタログに登録
		sm1 := Init()
		bp := sm1.BufferPool

		fileId, err := sm1.Catalog.AllocateFileId(bp)
		assert.NoError(t, err)
		err = sm1.RegisterDmToBp(fileId, "users")
		assert.NoError(t, err)

		metaPageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		tbl := access.NewTableAccessMethod("users", metaPageId, 1, nil)
		err = tbl.Create(bp)
		assert.NoError(t, err)

		cols := []*catalog.ColumnMetadata{
			catalog.NewColumnMetadata(fileId, "id", 0, catalog.ColumnTypeString),
		}
		tblMeta := catalog.NewTableMetadata(fileId, "users", 1, 1, cols, nil, metaPageId)
		err = sm1.Catalog.Insert(bp, tblMeta)
		assert.NoError(t, err)

		err = bp.FlushPage()
		assert.NoError(t, err)

		Reset()

		// WHEN: 同じディレクトリで再初期化
		sm2 := Init()

		// THEN: テーブルの Disk が登録されている
		dm, err := sm2.BufferPool.GetDisk(fileId)
		assert.NoError(t, err)
		assert.NotNil(t, dm)

		// カタログからテーブル情報も取得できる
		tableMeta, ok := sm2.Catalog.GetTableMetadataByName("users")
		assert.True(t, ok)
		assert.NotNil(t, tableMeta)
		assert.Equal(t, "users", tableMeta.Name)
	})

	t.Run("カタログファイルが空の場合、再作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// 空のカタログファイルを作成
		catalogPath := filepath.Join(tmpdir, "minesql.db")
		err := os.WriteFile(catalogPath, []byte{}, 0600)
		assert.NoError(t, err)

		// WHEN
		e := Init()

		// THEN: 新しいカタログが作成され、NextFileId は 1
		assert.NotNil(t, e)
		assert.NotNil(t, e.Catalog)
		assert.Equal(t, storage.FileId(1), e.Catalog.NextFileId)
	})

	t.Run("データディレクトリが存在しない場合、自動作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		nestedDir := filepath.Join(tmpdir, "nested", "data")
		t.Setenv("MINESQL_DATA_DIR", nestedDir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		e := Init()

		// THEN: ディレクトリが作成され、初期化が完了している
		assert.NotNil(t, e)
		_, err := os.Stat(nestedDir)
		assert.NoError(t, err)
	})
}

package access

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	t.Run("テーブルを新規作成できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTableTestEnv(t)
		input := CreateTableInput{
			TableName: "users",
			ColNames:  []string{"id", "name", "email"},
			PkCount:   1,
			Indexes: []CreateIndexInput{
				{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: catalog.IndexTypeNonUnique},
			},
		}

		// WHEN
		table, err := CreateTable(env.bp, env.undoLog, env.lockMgr, input)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, table)
		assert.NotNil(t, table.primaryIndex)
		assert.Len(t, table.secondaryIndexes, 1)
		assert.NotNil(t, table.undoLog)
		assert.NotNil(t, table.lock)
	})

	t.Run("作成したテーブルで DML を実行できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTableTestEnv(t)
		input := CreateTableInput{
			TableName: "users",
			ColNames:  []string{"id", "name", "email"},
			PkCount:   1,
			Indexes: []CreateIndexInput{
				{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: catalog.IndexTypeNonUnique},
			},
		}
		table, err := CreateTable(env.bp, env.undoLog, env.lockMgr, input)
		assert.NoError(t, err)

		// WHEN
		err = table.Insert(
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
			tableTrxId,
		)

		// THEN
		assert.NoError(t, err)
		iter, err := table.primaryIndex.search(SearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []string{"1", "Alice", "alice@example.com"}, record.Values)
	})

	t.Run("制約付きテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTableTestEnv(t)

		// 参照先テーブルを先に作成
		refInput := CreateTableInput{
			TableName: "departments",
			ColNames:  []string{"id", "name"},
			PkCount:   1,
		}
		_, err := CreateTable(env.bp, env.undoLog, env.lockMgr, refInput)
		assert.NoError(t, err)

		// WHEN
		input := CreateTableInput{
			TableName: "employees",
			ColNames:  []string{"id", "name", "dept_id"},
			PkCount:   1,
			Indexes: []CreateIndexInput{
				{IndexName: "idx_dept_id", ColNames: []string{"dept_id"}, IndexType: catalog.IndexTypeNonUnique},
			},
			Constraints: []CreateConstraintInput{
				{
					ColName:        "dept_id",
					ConstraintName: "fk_dept",
					RefTableName:   "departments",
					RefColName:     "id",
				},
			},
		}
		table, err := CreateTable(env.bp, env.undoLog, env.lockMgr, input)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, table)
		assert.Len(t, table.secondaryIndexes, 1)
	})

	t.Run("複数セカンダリインデックス付きテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTableTestEnv(t)
		input := CreateTableInput{
			TableName: "users",
			ColNames:  []string{"id", "name", "email"},
			PkCount:   1,
			Indexes: []CreateIndexInput{
				{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: catalog.IndexTypeNonUnique},
				{IndexName: "idx_email", ColNames: []string{"email"}, IndexType: catalog.IndexTypeUnique},
			},
		}

		// WHEN
		table, err := CreateTable(env.bp, env.undoLog, env.lockMgr, input)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, table.secondaryIndexes, 2)
	})

	t.Run("制約の参照先テーブルが存在しない場合エラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupCreateTableTestEnv(t)
		input := CreateTableInput{
			TableName: "employees",
			ColNames:  []string{"id", "dept_id"},
			PkCount:   1,
			Constraints: []CreateConstraintInput{
				{
					ColName:        "dept_id",
					ConstraintName: "fk_dept",
					RefTableName:   "nonexistent",
					RefColName:     "id",
				},
			},
		}

		// WHEN
		_, err := CreateTable(env.bp, env.undoLog, env.lockMgr, input)

		// THEN
		assert.Error(t, err)
	})

	t.Run("セカンダリインデックスなし・制約なしで作成できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTableTestEnv(t)
		input := CreateTableInput{
			TableName: "simple",
			ColNames:  []string{"id", "value"},
			PkCount:   1,
		}

		// WHEN
		table, err := CreateTable(env.bp, env.undoLog, env.lockMgr, input)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, table)
		assert.Empty(t, table.secondaryIndexes)
	})
}

func TestCreate(t *testing.T) {
	t.Run("テーブルのファイルを作成しバッファプールに登録できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnv(t)
		_ = os.MkdirAll(config.BaseDir, 0o750)
		t.Cleanup(func() {
			_ = os.Remove(filepath.Join(config.BaseDir, "test_table.db"))
			_ = os.Remove(config.BaseDir)
		})

		// WHEN
		fileId, err := createTableFile(env.ct, env.bp, "test_table")

		// THEN
		assert.NoError(t, err)
		assert.NotEqual(t, page.FileId(0), fileId)
	})

	t.Run("プライマリインデックスを作成できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnv(t)
		input := CreateTableInput{
			TableName: "users",
			ColNames:  []string{"id", "name", "email"},
			PkCount:   1,
		}

		// WHEN
		pi, err := createPrimaryIndex(env.ct, env.bp, env.fileId, input.PkCount, env.lockMgr)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, pi)
		assert.Equal(t, 1, pi.pkCount)
	})

	t.Run("テーブルメタ・カラムメタをカタログに登録できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnv(t)
		input := CreateTableInput{
			TableName: "users",
			ColNames:  []string{"id", "name", "email"},
			PkCount:   1,
		}
		pi, err := createPrimaryIndex(env.ct, env.bp, env.fileId, input.PkCount, env.lockMgr)
		assert.NoError(t, err)

		// WHEN
		err = registerTableMeta(env.ct, env.fileId, pi, input)

		// THEN
		assert.NoError(t, err)

		tableRecord, err := fetchTable(env.ct, "users")
		assert.NoError(t, err)
		assert.Equal(t, "users", tableRecord.Name)
		assert.Equal(t, 3, tableRecord.NumOfCol)

		colDefs, err := fetchColumnDefs(env.ct, env.fileId)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(colDefs))
		assert.Equal(t, 0, colDefs["id"])
		assert.Equal(t, 1, colDefs["name"])
		assert.Equal(t, 2, colDefs["email"])
	})

	t.Run("pkCount が複数のプライマリインデックスを作成できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnv(t)
		input := CreateTableInput{
			TableName: "composite",
			ColNames:  []string{"k1", "k2", "val"},
			PkCount:   2,
		}

		// WHEN
		pi, err := createPrimaryIndex(env.ct, env.bp, env.fileId, input.PkCount, env.lockMgr)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, pi.pkCount)
	})

	t.Run("同一テーブル名で registerTableMeta を 2 回呼ぶとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnv(t)
		input := CreateTableInput{
			TableName: "users",
			ColNames:  []string{"id", "name"},
			PkCount:   1,
		}
		pi, err := createPrimaryIndex(env.ct, env.bp, env.fileId, input.PkCount, env.lockMgr)
		assert.NoError(t, err)
		err = registerTableMeta(env.ct, env.fileId, pi, input)
		assert.NoError(t, err)

		// WHEN
		err = registerTableMeta(env.ct, env.fileId, pi, input)

		// THEN
		assert.Error(t, err)
	})

	t.Run("セカンダリインデックスを作成しカタログに登録できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnvWithTable(t)
		inputs := []CreateIndexInput{
			{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: catalog.IndexTypeNonUnique},
			{IndexName: "idx_email", ColNames: []string{"email"}, IndexType: catalog.IndexTypeUnique},
		}

		// WHEN
		sis, err := createSecondaryIndexes(env.ct, env.bp, env.fileId, env.primaryTree, env.lockMgr, inputs)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, sis, 2)

		names := map[string]bool{}
		for _, si := range sis {
			names[si.indexName] = true
		}
		assert.True(t, names["idx_name"])
		assert.True(t, names["idx_email"])
	})

	t.Run("ユニークインデックスの unique が true になる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnvWithTable(t)
		inputs := []CreateIndexInput{
			{IndexName: "idx_email", ColNames: []string{"email"}, IndexType: catalog.IndexTypeUnique},
		}

		// WHEN
		sis, err := createSecondaryIndexes(env.ct, env.bp, env.fileId, env.primaryTree, env.lockMgr, inputs)

		// THEN
		assert.NoError(t, err)
		assert.True(t, sis[0].unique)
	})

	t.Run("非ユニークインデックスの unique が false になる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnvWithTable(t)
		inputs := []CreateIndexInput{
			{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: catalog.IndexTypeNonUnique},
		}

		// WHEN
		sis, err := createSecondaryIndexes(env.ct, env.bp, env.fileId, env.primaryTree, env.lockMgr, inputs)

		// THEN
		assert.NoError(t, err)
		assert.False(t, sis[0].unique)
	})

	t.Run("同一インデックス名で 2 回作成するとエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnvWithTable(t)
		inputs := []CreateIndexInput{
			{IndexName: "idx_name", ColNames: []string{"name"}, IndexType: catalog.IndexTypeNonUnique},
		}
		_, err := createSecondaryIndexes(env.ct, env.bp, env.fileId, env.primaryTree, env.lockMgr, inputs)
		assert.NoError(t, err)

		// WHEN
		_, err = createSecondaryIndexes(env.ct, env.bp, env.fileId, env.primaryTree, env.lockMgr, inputs)

		// THEN
		assert.Error(t, err)
	})

	t.Run("空のインデックスリストでは空スライスを返す", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnvWithTable(t)

		// WHEN
		sis, err := createSecondaryIndexes(env.ct, env.bp, env.fileId, env.primaryTree, env.lockMgr, nil)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, sis)
	})

	t.Run("制約をカタログに登録できる", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnvWithTable(t)
		inputs := []CreateConstraintInput{
			{
				ColName:        "manager_id",
				ConstraintName: "fk_manager",
				RefTableName:   "users",
				RefColName:     "id",
			},
		}

		// WHEN
		err := createConstraints(env.ct, env.fileId, inputs)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("参照先テーブルが存在しない場合はエラーを返す", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnvWithTable(t)
		inputs := []CreateConstraintInput{
			{
				ColName:        "user_id",
				ConstraintName: "fk_user",
				RefTableName:   "nonexistent",
				RefColName:     "id",
			},
		}

		// WHEN
		err := createConstraints(env.ct, env.fileId, inputs)

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("制約が空の場合は何もせず正常終了する", func(t *testing.T) {
		// GIVEN
		env := setupCreateTestEnvWithTable(t)

		// WHEN
		err := createConstraints(env.ct, env.fileId, nil)

		// THEN
		assert.NoError(t, err)
	})
}

// createTestEnv は Create テスト用の基本環境
type createTestEnv struct {
	ct      *catalog.Catalog
	bp      *buffer.BufferPool
	fileId  page.FileId
	lockMgr *lock.Manager
}

// createTestEnvWithTable はテーブル作成済みの Create テスト用環境
type createTestEnvWithTable struct {
	ct          *catalog.Catalog
	bp          *buffer.BufferPool
	fileId      page.FileId
	primaryTree *btree.Btree
	lockMgr     *lock.Manager
}

// setupCreateTestEnv はカタログとバッファプールのみの環境を構築する
func setupCreateTestEnv(t *testing.T) *createTestEnv {
	t.Helper()

	catalogPath := filepath.Join(t.TempDir(), "catalog.db")
	catalogHf, err := file.NewHeapFile(page.FileId(0), catalogPath)
	if err != nil {
		t.Fatalf("カタログ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = catalogHf.Close() })

	bp := buffer.NewBufferPool(page.PageSize * 50)
	bp.RegisterHeapFile(page.FileId(0), catalogHf)

	ct, err := catalog.CreateCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の作成に失敗: %v", err)
	}

	// テスト用テーブルの HeapFile
	dataPath := filepath.Join(t.TempDir(), "data.db")
	fileId, err := ct.AllocateFileId()
	if err != nil {
		t.Fatalf("FileId の採番に失敗: %v", err)
	}
	dataHf, err := file.NewHeapFile(fileId, dataPath)
	if err != nil {
		t.Fatalf("データ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = dataHf.Close() })
	bp.RegisterHeapFile(fileId, dataHf)

	lockMgr := lock.NewManager()

	return &createTestEnv{
		ct:      ct,
		bp:      bp,
		fileId:  fileId,
		lockMgr: lockMgr,
	}
}

// setupCreateTestEnvWithTable はプライマリインデックス付きの "users" テーブルが存在する環境を構築する
func setupCreateTestEnvWithTable(t *testing.T) *createTestEnvWithTable {
	t.Helper()

	env := setupCreateTestEnv(t)
	input := CreateTableInput{
		TableName: "users",
		ColNames:  []string{"id", "name", "email"},
		PkCount:   1,
	}
	pi, err := createPrimaryIndex(env.ct, env.bp, env.fileId, input.PkCount, env.lockMgr)
	if err != nil {
		t.Fatalf("プライマリインデックスの作成に失敗: %v", err)
	}
	if err := registerTableMeta(env.ct, env.fileId, pi, input); err != nil {
		t.Fatalf("テーブルメタの登録に失敗: %v", err)
	}

	return &createTestEnvWithTable{
		ct:          env.ct,
		bp:          env.bp,
		fileId:      env.fileId,
		primaryTree: pi.tree,
		lockMgr:     env.lockMgr,
	}
}

// createTableTestEnv は CreateTable テスト用環境
type createTableTestEnv struct {
	bp      *buffer.BufferPool
	undoLog *undo.Manager
	lockMgr *lock.Manager
}

// setupCreateTableTestEnv は CreateTable の統合テスト用環境を構築する
func setupCreateTableTestEnv(t *testing.T) *createTableTestEnv {
	t.Helper()

	// config.BaseDir (= "data") にファイルが作られるので事前にディレクトリを用意
	_ = os.MkdirAll(config.BaseDir, 0o750)
	t.Cleanup(func() { _ = os.RemoveAll(config.BaseDir) })

	catalogPath := filepath.Join(t.TempDir(), "catalog.db")
	catalogHf, err := file.NewHeapFile(page.FileId(0), catalogPath)
	if err != nil {
		t.Fatalf("カタログ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = catalogHf.Close() })

	bp := buffer.NewBufferPool(page.PageSize * 50)
	bp.RegisterHeapFile(page.FileId(0), catalogHf)

	_, err = catalog.CreateCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の作成に失敗: %v", err)
	}

	// Undo 用 HeapFile
	undoPath := filepath.Join(t.TempDir(), "undo.db")
	undoHf, err := file.NewHeapFile(page.FileId(1), undoPath)
	if err != nil {
		t.Fatalf("Undo HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = undoHf.Close() })
	bp.RegisterHeapFile(page.FileId(1), undoHf)

	undoMgr, err := undo.NewManager(bp, page.FileId(1))
	if err != nil {
		t.Fatalf("undo.Manager の作成に失敗: %v", err)
	}

	lockMgr := lock.NewManager()

	return &createTableTestEnv{
		bp:      bp,
		undoLog: undoMgr,
		lockMgr: lockMgr,
	}
}

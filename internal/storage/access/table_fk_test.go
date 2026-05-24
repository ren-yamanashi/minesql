package access

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/catalog"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/undo"
	"github.com/stretchr/testify/assert"
)

const fkTrxId lock.TrxId = 1

func TestTableCheckForeignKeysForInsert(t *testing.T) {
	t.Run("FK の参照先に値が存在する場合、挿入が成功する", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)

		// WHEN
		err := env.child.Insert([]string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"}, fkTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("FK の参照先に値が存在しない場合、ErrForeignKeyViolation を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)

		// WHEN
		err := env.child.Insert([]string{"id", "name", "dept_id"}, []string{"1", "Alice", "999"}, fkTrxId)

		// THEN
		assert.ErrorIs(t, err, ErrForeignKeyViolation)
	})

	t.Run("FK の参照先が論理削除済みの場合、ErrForeignKeyViolation を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)
		record := searchFirstPrimaryRecord(t, env.parent)
		_ = env.parent.SoftDelete(record, fkTrxId)

		// WHEN
		err := env.child.Insert([]string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"}, fkTrxId)

		// THEN
		assert.ErrorIs(t, err, ErrForeignKeyViolation)
	})

	t.Run("FK 制約のないテーブルでは FK チェックが行われない", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)

		// WHEN (FK を持たない親テーブルへの挿入)
		err := env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)

		// THEN
		assert.NoError(t, err)
	})
}

func TestTableCheckForeignKeysForDelete(t *testing.T) {
	t.Run("子テーブルから参照されていないレコードの削除は成功する", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)
		record := searchFirstPrimaryRecord(t, env.parent)

		// WHEN
		err := env.parent.SoftDelete(record, fkTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("子テーブルから参照されているレコードの削除は ErrForeignKeyViolation を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)
		_ = env.child.Insert([]string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"}, fkTrxId)
		record := searchFirstPrimaryRecord(t, env.parent)

		// WHEN
		err := env.parent.SoftDelete(record, fkTrxId)

		// THEN
		assert.ErrorIs(t, err, ErrForeignKeyViolation)
	})

	t.Run("子テーブルの参照レコードが論理削除済みの場合、親の削除は成功する", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)
		_ = env.child.Insert([]string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"}, fkTrxId)
		childRecord := searchFirstPrimaryRecord(t, env.child)
		_ = env.child.SoftDelete(childRecord, fkTrxId)
		parentRecord := searchFirstPrimaryRecord(t, env.parent)

		// WHEN
		err := env.parent.SoftDelete(parentRecord, fkTrxId)

		// THEN
		assert.NoError(t, err)
	})
}

func TestTableCheckForeignKeysForUpdate(t *testing.T) {
	t.Run("FK カラムを有効な値に更新する場合は成功する", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"2", "Engineering"}, fkTrxId)
		_ = env.child.Insert([]string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"}, fkTrxId)
		record := searchFirstPrimaryRecord(t, env.child)

		// WHEN
		err := env.child.Update(record, []string{"dept_id"}, []string{"2"}, fkTrxId)

		// THEN
		assert.NoError(t, err)
	})

	t.Run("FK カラムを無効な値に更新する場合は ErrForeignKeyViolation を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)
		_ = env.child.Insert([]string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"}, fkTrxId)
		record := searchFirstPrimaryRecord(t, env.child)

		// WHEN
		err := env.child.Update(record, []string{"dept_id"}, []string{"999"}, fkTrxId)

		// THEN
		assert.ErrorIs(t, err, ErrForeignKeyViolation)
	})

	t.Run("親テーブルの参照されている PK を更新すると ErrForeignKeyViolation を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)
		_ = env.child.Insert([]string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"}, fkTrxId)
		record := searchFirstPrimaryRecord(t, env.parent)

		// WHEN (PK 更新 → SoftDelete + Insert に分岐 → SoftDelete で FK 違反)
		err := env.parent.Update(record, []string{"id"}, []string{"2"}, fkTrxId)

		// THEN
		assert.ErrorIs(t, err, ErrForeignKeyViolation)
	})

	t.Run("FK カラム以外のカラムのみ更新する場合は FK チェックが行われない", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)
		_ = env.parent.Insert([]string{"id", "name"}, []string{"1", "Sales"}, fkTrxId)
		_ = env.child.Insert([]string{"id", "name", "dept_id"}, []string{"1", "Alice", "1"}, fkTrxId)
		record := searchFirstPrimaryRecord(t, env.child)

		// WHEN
		err := env.child.Update(record, []string{"name"}, []string{"Bob"}, fkTrxId)

		// THEN
		assert.NoError(t, err)
	})
}

func TestFetchForeignKeys(t *testing.T) {
	t.Run("FK 制約がある場合、制約一覧を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)

		// WHEN
		fks, err := fetchForeignKeys(env.ct, env.childFileId)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, fks, 1)
		assert.Equal(t, "dept_id", fks[0].ColumnName())
	})

	t.Run("FK 制約がない場合、空のスライスを返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)

		// WHEN
		fks, err := fetchForeignKeys(env.ct, env.parentFileId)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, fks)
	})
}

func TestFetchReferencingConstraints(t *testing.T) {
	t.Run("親として参照されている場合、制約一覧を返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)

		// WHEN
		refs, err := fetchReferencingConstraints(env.ct, env.parentFileId)

		// THEN
		assert.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.Equal(t, "id", refs[0].ReferenceColumnName())
	})

	t.Run("親として参照されていない場合、空のスライスを返す", func(t *testing.T) {
		// GIVEN
		env := setupFKTestEnv(t)

		// WHEN
		refs, err := fetchReferencingConstraints(env.ct, env.childFileId)

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, refs)
	})
}

// fkTestEnv は FK テスト用の環境
type fkTestEnv struct {
	ct           *catalog.Catalog
	bp           *buffer.Pool
	parent       *Table // departments (id PK, name)
	child        *Table // employees (id PK, name, dept_id FK -> departments.id)
	parentFileId page.FileId
	childFileId  page.FileId
}

// setupFKTestEnv は FK テスト用の環境を構築する
//
// テーブル:
//   - departments (id PK, name)
//   - employees (id PK, name, dept_id FK -> departments.id)
//   - idx_dept_id: セカンダリインデックス (dept_id)
func setupFKTestEnv(t *testing.T) *fkTestEnv {
	t.Helper()

	_ = os.MkdirAll(config.BaseDir, 0o750)
	t.Cleanup(func() { _ = os.RemoveAll(config.BaseDir) })

	catalogPath := filepath.Join(t.TempDir(), "catalog.db")
	catalogHf, err := file.NewHeapFile(page.FileId(0), catalogPath)
	if err != nil {
		t.Fatalf("カタログ HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = catalogHf.Close() })

	bp := buffer.NewPool(page.Size * 50)
	bp.RegisterHeapFile(page.FileId(0), catalogHf)

	ct, err := catalog.CreateCatalog(bp)
	if err != nil {
		t.Fatalf("Catalog の作成に失敗: %v", err)
	}

	// Undo 用 HeapFile
	undoFileId := ct.UndoLogFileId()
	undoPath := filepath.Join(config.BaseDir, "undo.db")
	undoHf, err := file.NewHeapFile(undoFileId, undoPath)
	if err != nil {
		t.Fatalf("Undo HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = undoHf.Close() })
	bp.RegisterHeapFile(undoFileId, undoHf)

	undoMgr, err := undo.NewManager(bp, nil, undoFileId)
	if err != nil {
		t.Fatalf("undo.Manager の作成に失敗: %v", err)
	}

	lockMgr := lock.NewManager()

	// 親テーブル: departments
	parentTable, err := CreateTable(bp, undoMgr, lockMgr, CreateTableInput{
		TableName: "departments",
		ColNames:  []string{"id", "name"},
		PkCount:   1,
	})
	if err != nil {
		t.Fatalf("departments テーブルの作成に失敗: %v", err)
	}
	parentFileId := parentTable.primaryIndex.fileId()

	// 子テーブル: employees (FK: dept_id -> departments.id)
	childTable, err := CreateTable(bp, undoMgr, lockMgr, CreateTableInput{
		TableName: "employees",
		ColNames:  []string{"id", "name", "dept_id"},
		PkCount:   1,
		Indexes: []CreateIndexInput{
			{IndexName: "idx_dept_id", ColNames: []string{"dept_id"}, IndexType: catalog.IndexTypeNonUnique},
		},
		Constraints: []CreateConstraintInput{
			{
				ColumnName:          "dept_id",
				ConstraintName:      "fk_dept",
				ReferenceTableName:  "departments",
				ReferenceColumnName: "id",
			},
		},
	})
	if err != nil {
		t.Fatalf("employees テーブルの作成に失敗: %v", err)
	}
	childFileId := childTable.primaryIndex.fileId()

	return &fkTestEnv{
		ct:           ct,
		bp:           bp,
		parent:       parentTable,
		child:        childTable,
		parentFileId: parentFileId,
		childFileId:  childFileId,
	}
}

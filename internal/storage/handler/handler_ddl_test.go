package handler

import (
	"minesql/internal/storage/lock"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	t.Run("テーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		}, nil)

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.Equal(t, "users", meta.Name)
		assert.Equal(t, uint8(2), meta.NCols)
		assert.Equal(t, uint8(1), meta.PKCount)

		// PK 制約が自動生成されている
		assert.Equal(t, 1, len(meta.Constraints))
		assert.Equal(t, "id", meta.Constraints[0].ColName)
		assert.Equal(t, "PRIMARY", meta.Constraints[0].ConstraintName)
		assert.Equal(t, "", meta.Constraints[0].RefTableName)
	})

	t.Run("ユニークインデックス付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		err := h.CreateTable("users", 1,
			[]CreateIndexParam{
				{Name: "idx_email", ColName: "email", ColIdx: 1, Unique: true},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "email", Type: ColumnTypeString},
			},
			nil,
		)

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.Equal(t, 1, len(meta.Indexes))
		assert.Equal(t, "idx_email", meta.Indexes[0].Name)

		// PK 制約と UK 制約が自動生成されている
		assert.Equal(t, 2, len(meta.Constraints))
		assert.Equal(t, "id", meta.Constraints[0].ColName)
		assert.Equal(t, "PRIMARY", meta.Constraints[0].ConstraintName)
		assert.Equal(t, "email", meta.Constraints[1].ColName)
		assert.Equal(t, "idx_email", meta.Constraints[1].ConstraintName)
	})

	t.Run("非ユニークインデックス付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		err := h.CreateTable("products", 1,
			[]CreateIndexParam{
				{Name: "idx_category", ColName: "category", ColIdx: 1, Unique: false},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "category", Type: ColumnTypeString},
			},
			nil,
		)

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("products")
		assert.True(t, ok)
		assert.Equal(t, 1, len(meta.Indexes))
		assert.Equal(t, "idx_category", meta.Indexes[0].Name)
		assert.Equal(t, "non-unique secondary", string(meta.Indexes[0].Type))

		// PK 制約のみ自動生成されている (非ユニークインデックスは制約にならない)
		assert.Equal(t, 1, len(meta.Constraints))
		assert.Equal(t, "PRIMARY", meta.Constraints[0].ConstraintName)
	})

	t.Run("ユニークと非ユニークのインデックスを同時に作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// WHEN
		err := h.CreateTable("products", 1,
			[]CreateIndexParam{
				{Name: "idx_name", ColName: "name", ColIdx: 1, Unique: true},
				{Name: "idx_category", ColName: "category", ColIdx: 2, Unique: false},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "name", Type: ColumnTypeString},
				{Name: "category", Type: ColumnTypeString},
			},
			nil,
		)

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("products")
		assert.True(t, ok)
		assert.Equal(t, 2, len(meta.Indexes))
		assert.Equal(t, "unique secondary", string(meta.Indexes[0].Type))
		assert.Equal(t, "non-unique secondary", string(meta.Indexes[1].Type))

		// PK 制約と UK 制約が自動生成されている (非ユニークインデックスは制約にならない)
		assert.Equal(t, 2, len(meta.Constraints))
		assert.Equal(t, "PRIMARY", meta.Constraints[0].ConstraintName)
		assert.Equal(t, "idx_name", meta.Constraints[1].ConstraintName)
	})

	t.Run("非ユニークインデックス付きテーブルに同一キーの複数レコードを挿入できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("products", 1,
			[]CreateIndexParam{
				{Name: "idx_category", ColName: "category", ColIdx: 1, Unique: false},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "category", Type: ColumnTypeString},
			},
			nil,
		)
		assert.NoError(t, err)

		tbl, err := h.GetTable("products")
		assert.NoError(t, err)

		// WHEN: 同一カテゴリの複数レコードを挿入
		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("1"), []byte("Fruit")})
		assert.NoError(t, err)
		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("2"), []byte("Fruit")})
		assert.NoError(t, err)
		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("3"), []byte("Veggie")})

		// THEN: ユニーク制約エラーにならない
		assert.NoError(t, err)
	})

	t.Run("作成したテーブルにレコードを挿入できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		}, nil)
		assert.NoError(t, err)

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		// WHEN
		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("1"), []byte("Alice")})

		// THEN
		assert.NoError(t, err)
	})

	t.Run("PK, UK, FK 制約が混在するテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// 親テーブルを作成
		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
		}, []CreateConstraintParam{})
		assert.NoError(t, err)

		// WHEN: PK + UK + FK 制約を持つテーブルを作成
		err = h.CreateTable("orders", 1,
			[]CreateIndexParam{
				{Name: "idx_code", ColName: "code", ColIdx: 1, Unique: true},
				{Name: "idx_user_id", ColName: "user_id", ColIdx: 2, Unique: false},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "code", Type: ColumnTypeString},
				{Name: "user_id", Type: ColumnTypeString},
			},
			[]CreateConstraintParam{
				{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"},
			},
		)

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("orders")
		assert.True(t, ok)

		// PK (id) + UK (code) + FK (user_id) の 3 制約
		assert.Equal(t, 3, len(meta.Constraints))

		// PK 制約
		assert.Equal(t, "id", meta.Constraints[0].ColName)
		assert.Equal(t, "PRIMARY", meta.Constraints[0].ConstraintName)
		assert.Equal(t, "", meta.Constraints[0].RefTableName)

		// UK 制約 (ユニークインデックス名が制約名になる)
		assert.Equal(t, "code", meta.Constraints[1].ColName)
		assert.Equal(t, "idx_code", meta.Constraints[1].ConstraintName)
		assert.Equal(t, "", meta.Constraints[1].RefTableName)

		// FK 制約
		assert.Equal(t, "user_id", meta.Constraints[2].ColName)
		assert.Equal(t, "fk_user", meta.Constraints[2].ConstraintName)
		assert.Equal(t, "users", meta.Constraints[2].RefTableName)
		assert.Equal(t, "id", meta.Constraints[2].RefColName)

		// GetForeignKeyConstraints で FK のみ取得できる
		fks := meta.GetForeignKeyConstraints()
		assert.Equal(t, 1, len(fks))
		assert.Equal(t, "fk_user", fks[0].ConstraintName)
	})

	t.Run("FK 制約付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// 親テーブルを作成
		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		}, []CreateConstraintParam{})
		assert.NoError(t, err)

		// WHEN: FK 制約付きの子テーブルを作成
		err = h.CreateTable("orders", 1,
			[]CreateIndexParam{
				{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false},
			},
			[]CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "user_id", Type: ColumnTypeString},
			},
			[]CreateConstraintParam{
				{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"},
			},
		)

		// THEN
		assert.NoError(t, err)
		meta, ok := h.Catalog.GetTableMetaByName("orders")
		assert.True(t, ok)

		// PK 制約 + FK 制約が登録されている
		assert.Equal(t, 2, len(meta.Constraints))
		assert.Equal(t, "id", meta.Constraints[0].ColName)
		assert.Equal(t, "PRIMARY", meta.Constraints[0].ConstraintName)
		assert.Equal(t, "", meta.Constraints[0].RefTableName)

		assert.Equal(t, "user_id", meta.Constraints[1].ColName)
		assert.Equal(t, "fk_user", meta.Constraints[1].ConstraintName)
		assert.Equal(t, "users", meta.Constraints[1].RefTableName)
		assert.Equal(t, "id", meta.Constraints[1].RefColName)

		// GetForeignKeyConstraints で FK のみ取得できる
		fks := meta.GetForeignKeyConstraints()
		assert.Equal(t, 1, len(fks))
		assert.Equal(t, "fk_user", fks[0].ConstraintName)
	})
}

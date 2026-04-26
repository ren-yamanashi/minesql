package executor

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/handler"
	"github.com/stretchr/testify/assert"
)

func TestNewCreateTable(t *testing.T) {
	t.Run("インデックスとカラムと制約のパラメータが nil の場合に空のスライスに変換される", func(t *testing.T) {
		// WHEN
		createTable := NewCreateTable("users", 1, nil, nil, nil)

		// THEN
		assert.NotNil(t, createTable.indexParams)
		assert.NotNil(t, createTable.columnParams)
		assert.NotNil(t, createTable.constraintParams)
		assert.Equal(t, 0, len(createTable.indexParams))
		assert.Equal(t, 0, len(createTable.columnParams))
		assert.Equal(t, 0, len(createTable.constraintParams))
	})
}

func TestCreateTable_Next(t *testing.T) {
	t.Run("テーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		createTable := NewCreateTable("users", 1, nil, nil, nil)

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, ok := hdl.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.NotNil(t, tblMeta)
		assert.Equal(t, "users", tblMeta.Name)
		assert.Equal(t, uint8(1), tblMeta.PKCount)
	})

	t.Run("カラムを指定してテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		createTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "email", Type: "string"},
		}, nil)

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, ok := hdl.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.NotNil(t, tblMeta)
		assert.Equal(t, uint8(3), tblMeta.NCols)
		assert.Equal(t, 3, len(tblMeta.Cols))
		assert.Equal(t, "id", tblMeta.Cols[0].Name)
		assert.Equal(t, "int", string(tblMeta.Cols[0].Type))
		assert.Equal(t, uint16(0), tblMeta.Cols[0].Pos)
		assert.Equal(t, "name", tblMeta.Cols[1].Name)
		assert.Equal(t, "string", string(tblMeta.Cols[1].Type))
		assert.Equal(t, uint16(1), tblMeta.Cols[1].Pos)
		assert.Equal(t, "email", tblMeta.Cols[2].Name)
		assert.Equal(t, "string", string(tblMeta.Cols[2].Type))
		assert.Equal(t, uint16(2), tblMeta.Cols[2].Pos)
	})

	t.Run("ユニークインデックス付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		createTable := NewCreateTable("users", 1, []handler.CreateIndexParam{
			{Name: "email", ColName: "email", ColIdx: 1, Unique: true},
		}, nil, nil)

		// WHEN
		_, err := createTable.Next()
		assert.NoError(t, err)

		// THEN
		tblMeta, ok := hdl.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.NotNil(t, tblMeta)
		assert.Equal(t, 1, len(tblMeta.Indexes))
		assert.Equal(t, "email", tblMeta.Indexes[0].ColName)
	})

	t.Run("テーブルファイルが作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		createTable := NewCreateTable("users", 1, nil, nil, nil)

		// WHEN
		_, err := createTable.Next()
		assert.NoError(t, err)

		// THEN
		tblMeta, ok := hdl.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.NotNil(t, tblMeta)
		// ディスクマネージャが登録されていることを確認
		dm, dmErr := hdl.BufferPool.GetDisk(tblMeta.DataMetaPageId.FileId)
		assert.NoError(t, dmErr)
		assert.NotNil(t, dm)
	})

	t.Run("PK 制約が自動生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		createTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
			{Name: "name", Type: "string"},
		}, []handler.CreateConstraintParam{})

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, ok := hdl.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.Equal(t, 1, len(tblMeta.Constraints))
		assert.Equal(t, "id", tblMeta.Constraints[0].ColName)
		assert.Equal(t, "PRIMARY", tblMeta.Constraints[0].ConstraintName)
		assert.Equal(t, "", tblMeta.Constraints[0].RefTableName)
	})

	t.Run("UK 制約が自動生成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()
		createTable := NewCreateTable("users", 1,
			[]handler.CreateIndexParam{
				{Name: "idx_email", ColName: "email", ColIdx: 1, Unique: true},
			},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "email", Type: "string"},
			},
			[]handler.CreateConstraintParam{},
		)

		// WHEN
		_, err := createTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, ok := hdl.Catalog.GetTableMetaByName("users")
		assert.True(t, ok)
		assert.Equal(t, 2, len(tblMeta.Constraints))
		assert.Equal(t, "PRIMARY", tblMeta.Constraints[0].ConstraintName)
		assert.Equal(t, "idx_email", tblMeta.Constraints[1].ConstraintName)
		assert.Equal(t, "email", tblMeta.Constraints[1].ColName)
	})

	t.Run("FK 制約付きのテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()

		// 親テーブルを作成
		parentTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, []handler.CreateConstraintParam{})
		_, err := parentTable.Next()
		assert.NoError(t, err)

		// WHEN: FK 制約付きの子テーブルを作成
		childTable := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{
				{Name: "idx_user_id", ColName: "user_id", ColIdx: 1, Unique: false},
			},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "user_id", Type: "string"},
			},
			[]handler.CreateConstraintParam{
				{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"},
			},
		)
		_, err = childTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, ok := hdl.Catalog.GetTableMetaByName("orders")
		assert.True(t, ok)
		assert.Equal(t, 2, len(tblMeta.Constraints))

		// PK 制約
		assert.Equal(t, "PRIMARY", tblMeta.Constraints[0].ConstraintName)

		// FK 制約
		assert.Equal(t, "fk_user", tblMeta.Constraints[1].ConstraintName)
		assert.Equal(t, "user_id", tblMeta.Constraints[1].ColName)
		assert.Equal(t, "users", tblMeta.Constraints[1].RefTableName)
		assert.Equal(t, "id", tblMeta.Constraints[1].RefColName)
	})

	t.Run("PK, UK, FK 制約が混在するテーブルを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		handler.Reset()
		handler.Init()
		hdl := handler.Get()

		// 親テーブルを作成
		parentTable := NewCreateTable("users", 1, nil, []handler.CreateColumnParam{
			{Name: "id", Type: "string"},
		}, []handler.CreateConstraintParam{})
		_, err := parentTable.Next()
		assert.NoError(t, err)

		// WHEN: PK + UK + FK が混在するテーブルを作成
		childTable := NewCreateTable("orders", 1,
			[]handler.CreateIndexParam{
				{Name: "idx_code", ColName: "code", ColIdx: 1, Unique: true},
				{Name: "idx_user_id", ColName: "user_id", ColIdx: 2, Unique: false},
			},
			[]handler.CreateColumnParam{
				{Name: "id", Type: "string"},
				{Name: "code", Type: "string"},
				{Name: "user_id", Type: "string"},
			},
			[]handler.CreateConstraintParam{
				{ConstraintName: "fk_user", ColName: "user_id", RefTableName: "users", RefColName: "id"},
			},
		)
		_, err = childTable.Next()

		// THEN
		assert.NoError(t, err)
		tblMeta, ok := hdl.Catalog.GetTableMetaByName("orders")
		assert.True(t, ok)

		// PK + UK + FK の 3 制約
		assert.Equal(t, 3, len(tblMeta.Constraints))
		assert.Equal(t, "PRIMARY", tblMeta.Constraints[0].ConstraintName)
		assert.Equal(t, "idx_code", tblMeta.Constraints[1].ConstraintName)
		assert.Equal(t, "fk_user", tblMeta.Constraints[2].ConstraintName)
		assert.Equal(t, "users", tblMeta.Constraints[2].RefTableName)

		// GetForeignKeyConstraints で FK のみ取得できる
		fks := tblMeta.GetForeignKeyConstraints()
		assert.Equal(t, 1, len(fks))
		assert.Equal(t, "fk_user", fks[0].ConstraintName)
	})
}

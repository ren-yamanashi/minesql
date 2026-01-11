package storage

import (
	"minesql/internal/storage/access/btree"
	"minesql/internal/storage/access/table"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetIndexIterator(t *testing.T) {
	t.Run("インデックスイテレータを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		uniqueIndex := table.NewUniqueIndex("last_name", 2)
		tbl, _ := engine.CreateTable("users", 1, []*table.UniqueIndex{uniqueIndex})
		bpm := engine.GetBufferPoolManager()
		tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("Alice"), []byte("Smith")})
		tbl.Insert(bpm, [][]byte{[]byte("b"), []byte("Bob"), []byte("Johnson")})

		handler, _ := engine.GetTableHandler("users")

		// WHEN
		err := handler.SetIndexIterator("last_name", btree.SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, handler.iterator)
	})

	t.Run("作成したインデックスイテレータでレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		uniqueIndex := table.NewUniqueIndex("last_name", 2)
		tbl, _ := engine.CreateTable("users", 1, []*table.UniqueIndex{uniqueIndex})
		bpm := engine.GetBufferPoolManager()
		tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("Alice"), []byte("Smith")})

		handler, _ := engine.GetTableHandler("users")

		// WHEN
		err := handler.SetIndexIterator("last_name", btree.SearchModeStart{})
		pair, ok, err := handler.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, pair)
	})

	t.Run("存在しないインデックスでイテレータを作成するとエラー", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		tbl, _ := engine.CreateTable("users", 1, []*table.UniqueIndex{})
		bpm := engine.GetBufferPoolManager()
		tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("Alice")})

		handler, _ := engine.GetTableHandler("users")

		// WHEN
		err := handler.SetIndexIterator("nonexistent", btree.SearchModeStart{})

		// THEN
		assert.Error(t, err)
		assert.Nil(t, handler.iterator)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("SearchModeKey でインデックスイテレータを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		uniqueIndex := table.NewUniqueIndex("last_name", 2)
		tbl, _ := engine.CreateTable("users", 1, []*table.UniqueIndex{uniqueIndex})
		bpm := engine.GetBufferPoolManager()
		tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("Alice"), []byte("Smith")})
		tbl.Insert(bpm, [][]byte{[]byte("b"), []byte("Bob"), []byte("Johnson")})

		handler, _ := engine.GetTableHandler("users")

		var encodedKey []byte
		table.Encode([][]byte{[]byte("Smith")}, &encodedKey)

		// WHEN
		err := handler.SetIndexIterator("last_name", btree.SearchModeKey{Key: encodedKey})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, handler.iterator)
	})
}

func TestSetTableIterator(t *testing.T) {
	t.Run("テーブルイテレータを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		engine.CreateTable("users", 1, []*table.UniqueIndex{})

		handler, _ := engine.GetTableHandler("users")

		// WHEN
		err := handler.SetTableIterator(btree.SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, handler.iterator)
	})

	t.Run("作成したイテレータでレコードを取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		tbl, _ := engine.CreateTable("users", 1, []*table.UniqueIndex{})
		bpm := engine.GetBufferPoolManager()
		tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("Alice")})

		handler, _ := engine.GetTableHandler("users")

		// WHEN
		err := handler.SetTableIterator(btree.SearchModeStart{})
		pair, ok, err := handler.Next()

		// THEN
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, pair)
	})

	t.Run("SearchModeKey でイテレータを作成できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		tbl, _ := engine.CreateTable("users", 1, []*table.UniqueIndex{})
		bpm := engine.GetBufferPoolManager()
		tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("Alice")})
		tbl.Insert(bpm, [][]byte{[]byte("b"), []byte("Bob")})

		handler, _ := engine.GetTableHandler("users")

		var encodedKey []byte
		table.Encode([][]byte{[]byte("b")}, &encodedKey)

		// WHEN
		err := handler.SetTableIterator(btree.SearchModeKey{Key: encodedKey})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, handler.iterator)
	})

	t.Run("テーブルイテレータで検索できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		ResetStorageEngine()
		InitStorageEngine()
		engine := GetStorageEngine()
		tbl, _ := engine.CreateTable("users", 1, []*table.UniqueIndex{})
		bpm := engine.GetBufferPoolManager()
		tbl.Insert(bpm, [][]byte{[]byte("a"), []byte("Alice")})

		handler, _ := engine.GetTableHandler("users")

		// WHEN
		err := handler.SetTableIterator(btree.SearchModeStart{})

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, handler.iterator)
		pair, ok, err := handler.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, pair)
	})
}

package handler

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/lock"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBeginTrx(t *testing.T) {
	t.Run("トランザクション ID を採番できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()
		h := Init()

		// WHEN
		trxId := h.BeginTrx()

		// THEN
		assert.NotEqual(t, TrxId(0), trxId)
	})

	t.Run("連続で採番すると異なる ID が返る", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()
		h := Init()

		// WHEN
		trxId1 := h.BeginTrx()
		trxId2 := h.BeginTrx()

		// THEN
		assert.NotEqual(t, trxId1, trxId2)
	})
}

func TestCommitTrx(t *testing.T) {
	t.Run("コミット後に Undo ログが破棄される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
		})
		assert.NoError(t, err)
		meta, _ := h.Catalog.GetTableMetaByName("users")
		tbl, _ := meta.GetTable()

		trxId := h.BeginTrx()
		h.AppendInsertUndo(trxId, tbl, [][]byte{[]byte("1")})
		assert.Equal(t, 1, len(h.UndoLog().GetRecords(trxId)))

		// WHEN
		h.CommitTrx(trxId)

		// THEN
		assert.Nil(t, h.UndoLog().GetRecords(trxId))
	})
}

func TestRollbackTrx(t *testing.T) {
	t.Run("ロールバックで Insert が取り消される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		})
		assert.NoError(t, err)
		meta, _ := h.Catalog.GetTableMetaByName("users")
		tbl, _ := meta.GetTable()

		trxId := h.BeginTrx()
		h.AppendInsertUndo(trxId, tbl, [][]byte{[]byte("1"), []byte("Alice")})
		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN
		err = h.RollbackTrx(trxId)

		// THEN: Insert が取り消されてテーブルが空
		assert.NoError(t, err)
		iter, err := tbl.Search(h.BufferPool, 0, lock.NewManager(5000), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestAppendUndo(t *testing.T) {
	t.Run("AppendInsertUndo で Undo レコードが記録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
		})
		assert.NoError(t, err)
		meta, _ := h.Catalog.GetTableMetaByName("users")
		tbl, _ := meta.GetTable()
		trxId := h.BeginTrx()

		// WHEN
		h.AppendInsertUndo(trxId, tbl, [][]byte{[]byte("1")})

		// THEN
		records := h.UndoLog().GetRecords(trxId)
		assert.Equal(t, 1, len(records))
	})

	t.Run("AppendDeleteUndo で Undo レコードが記録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
		})
		assert.NoError(t, err)
		meta, _ := h.Catalog.GetTableMetaByName("users")
		tbl, _ := meta.GetTable()
		trxId := h.BeginTrx()

		// WHEN
		h.AppendDeleteUndo(trxId, tbl, [][]byte{[]byte("1")})

		// THEN
		records := h.UndoLog().GetRecords(trxId)
		assert.Equal(t, 1, len(records))
	})

	t.Run("AppendUpdateInplaceUndo で Undo レコードが記録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		})
		assert.NoError(t, err)
		meta, _ := h.Catalog.GetTableMetaByName("users")
		tbl, _ := meta.GetTable()
		trxId := h.BeginTrx()

		// WHEN
		h.AppendUpdateInplaceUndo(trxId, tbl,
			[][]byte{[]byte("1"), []byte("Alice")},
			[][]byte{[]byte("1"), []byte("Bob")},
		)

		// THEN
		records := h.UndoLog().GetRecords(trxId)
		assert.Equal(t, 1, len(records))
	})
}

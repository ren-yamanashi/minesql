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
	t.Run("コミット後もデータが永続化されている", func(t *testing.T) {
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

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		trxId := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trxId, h.LockMgr, [][]byte{[]byte("1")})
		assert.NoError(t, err)

		// WHEN
		h.CommitTrx(trxId)

		// THEN
		readTrxId := h.BeginTrx()
		defer h.CommitTrx(readTrxId)
		iter, err := tbl.Search(h.BufferPool, readTrxId, h.LockMgr, access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("1"), record[0])
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

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		trxId := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trxId, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
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

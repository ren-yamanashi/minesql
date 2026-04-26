package handler

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/access"
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
		}, nil)
		assert.NoError(t, err)

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		trxId := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trxId, h.LockMgr, [][]byte{[]byte("1")})
		assert.NoError(t, err)

		// WHEN
		err = h.CommitTrx(trxId)
		assert.NoError(t, err)

		// THEN
		readTrxId := h.BeginTrx()
		defer func() { assert.NoError(t, h.CommitTrx(readTrxId)) }()
		rv := h.CreateReadView(readTrxId)
		vr := access.NewVersionReader(h.UndoLog())
		iter, err := tbl.Search(h.BufferPool, rv, vr, access.RecordSearchModeStart{})
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
		}, nil)
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
		rv := access.NewReadView(0, nil, ^uint64(0))
		vr := access.NewVersionReader(nil)
		iter, err := tbl.Search(h.BufferPool, rv, vr, access.RecordSearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestCreateReadView(t *testing.T) {
	t.Run("トランザクション用の ReadView が作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()
		h := Init()

		trx1 := h.BeginTrx()
		trx2 := h.BeginTrx()

		// WHEN
		rv := h.CreateReadView(trx2)

		// THEN
		assert.Equal(t, trx2, rv.TrxId)
		assert.Contains(t, rv.MIds, trx1)    // trx1 はアクティブ
		assert.NotContains(t, rv.MIds, trx2) // 自分は含まれない
	})

	t.Run("コミット済みトランザクションは MIds に含まれない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()
		h := Init()

		trx1 := h.BeginTrx()
		err := h.CommitTrx(trx1)
		assert.NoError(t, err)

		trx2 := h.BeginTrx()

		// WHEN
		rv := h.CreateReadView(trx2)

		// THEN
		assert.NotContains(t, rv.MIds, trx1) // コミット済み
	})
}

func TestTransactionIsolation(t *testing.T) {
	t.Run("Consistent Read: 他トランザクションの未コミット INSERT は見えない", func(t *testing.T) {
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

		// T1 が行を INSERT (未コミット)
		trx1 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx1, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN: T2 が Consistent Read でテーブルを読み取る
		trx2 := h.BeginTrx()
		rv := h.CreateReadView(trx2)
		vr := access.NewVersionReader(h.UndoLog())
		iter, err := tbl.Search(h.BufferPool, rv, vr, access.RecordSearchModeStart{})
		assert.NoError(t, err)

		_, ok, err := iter.Next()
		assert.NoError(t, err)

		// THEN: T1 の未コミット INSERT は不可視
		assert.False(t, ok)
	})

	t.Run("Consistent Read: 他トランザクションのコミット済み INSERT は ReadView 作成時点に依存する", func(t *testing.T) {
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

		// T1 が行を INSERT してコミット
		trx1 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx1, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx1))

		// WHEN: T2 が ReadView を作成して読み取る (T1 コミット後なので可視)
		trx2 := h.BeginTrx()
		rv := h.CreateReadView(trx2)
		vr := access.NewVersionReader(h.UndoLog())
		iter, err := tbl.Search(h.BufferPool, rv, vr, access.RecordSearchModeStart{})
		assert.NoError(t, err)

		record, ok, err := iter.Next()
		assert.NoError(t, err)

		// THEN: T1 のコミット済み INSERT は可視
		assert.True(t, ok)
		assert.Equal(t, "Alice", string(record[1]))
	})

	t.Run("Current Read: UPDATE は他トランザクションのコミット済み行を最新バージョンで読む", func(t *testing.T) {
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

		// T1 が行を INSERT してコミット
		trx1 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx1, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx1))

		// T2 が開始 (T1 コミット後)
		trx2 := h.BeginTrx()

		// T3 が行を UPDATE してコミット (T2 の開始後)
		trx3 := h.BeginTrx()
		err = tbl.UpdateInplace(h.BufferPool, trx3, h.LockMgr,
			[][]byte{[]byte("1"), []byte("Alice")},
			[][]byte{[]byte("1"), []byte("Bob")},
		)
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx3))

		// WHEN: T2 が Current Read で読む (全可視 ReadView)
		rv := access.NewReadView(0, nil, ^uint64(0))
		vr := access.NewVersionReader(nil)
		iter, err := tbl.Search(h.BufferPool, rv, vr, access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)

		// THEN: T3 が更新した最新バージョン "Bob" が見える
		assert.True(t, ok)
		assert.Equal(t, "Bob", string(record[1]))
		assert.NoError(t, h.CommitTrx(trx2))
	})

	t.Run("Consistent Read: UPDATE 前の旧バージョンが undo チェーン経由で見える", func(t *testing.T) {
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

		// T1 が行を INSERT してコミット
		trx1 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx1, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx1))

		// T2 が開始し ReadView を作成 (この時点では name=Alice)
		trx2 := h.BeginTrx()
		rv := h.CreateReadView(trx2)

		// T3 が行を UPDATE してコミット (T2 の ReadView 作成後)
		trx3 := h.BeginTrx()
		err = tbl.UpdateInplace(h.BufferPool, trx3, h.LockMgr,
			[][]byte{[]byte("1"), []byte("Alice")},
			[][]byte{[]byte("1"), []byte("Bob")},
		)
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx3))

		// WHEN: T2 が Consistent Read で読む (ReadView は T3 コミット前に作成済み)
		vr := access.NewVersionReader(h.UndoLog())
		iter, err := tbl.Search(h.BufferPool, rv, vr, access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)

		// THEN: T3 の更新は不可視。undo チェーンを辿って T1 が書いた "Alice" が返る
		assert.True(t, ok)
		assert.Equal(t, "Alice", string(record[1]))
		assert.NoError(t, h.CommitTrx(trx2))
	})
}

func TestPurgeThreadIntegration(t *testing.T) {
	t.Run("DELETE → COMMIT 後にパージスレッドが delete-marked レコードを物理削除する", func(t *testing.T) {
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

		// INSERT → COMMIT
		trx1 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx1, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx1))

		// DELETE → COMMIT
		trx2 := h.BeginTrx()
		err = tbl.SoftDelete(h.BufferPool, trx2, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx2))

		// WHEN: パージを直接実行 (バックグラウンドスレッドを待つ代わりに)
		pt := h.purgeThread
		purgeLimit := h.trxManager.PurgeLimit()
		committedIds := h.trxManager.CommittedTrxIds()
		err = pt.RunPurge(purgeLimit, committedIds)
		assert.NoError(t, err)

		// THEN: テーブルが空 (物理削除済み)
		rv := access.NewReadView(0, nil, ^uint64(0))
		vr := access.NewVersionReader(nil)
		iter, err := tbl.Search(h.BufferPool, rv, vr, access.RecordSearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("アクティブな ReadView がある場合はパージされない", func(t *testing.T) {
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

		// INSERT → COMMIT
		trx1 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx1, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx1))

		// T2 が ReadView を作成 (DELETE 前の状態を保持)
		trx2 := h.BeginTrx()
		h.CreateReadView(trx2)

		// T3 が DELETE → COMMIT
		trx3 := h.BeginTrx()
		err = tbl.SoftDelete(h.BufferPool, trx3, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx3))

		// WHEN: パージを実行
		pt := h.purgeThread
		purgeLimit := h.trxManager.PurgeLimit()
		committedIds := h.trxManager.CommittedTrxIds()
		err = pt.RunPurge(purgeLimit, committedIds)
		assert.NoError(t, err)

		// THEN: T2 の ReadView があるのでパージされない (Consistent Read で元の行が見える)
		rv := h.CreateReadView(trx2)
		vr := access.NewVersionReader(h.UndoLog())
		iter, err := tbl.Search(h.BufferPool, rv, vr, access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Alice", string(record[1]))

		assert.NoError(t, h.CommitTrx(trx2))
	})

	t.Run("UPDATE → COMMIT 後にパージスレッドが undo ログを破棄する", func(t *testing.T) {
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

		// INSERT → COMMIT
		trx1 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx1, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx1))

		// UPDATE → COMMIT
		trx2 := h.BeginTrx()
		err = tbl.UpdateInplace(h.BufferPool, trx2, h.LockMgr,
			[][]byte{[]byte("1"), []byte("Alice")},
			[][]byte{[]byte("1"), []byte("Bob")},
		)
		assert.NoError(t, err)
		assert.NoError(t, h.CommitTrx(trx2))

		// UPDATE の undo ログが残っている
		assert.NotNil(t, h.UndoLog().GetRecords(trx2))

		// WHEN: パージを実行
		pt := h.purgeThread
		purgeLimit := h.trxManager.PurgeLimit()
		committedIds := h.trxManager.CommittedTrxIds()
		err = pt.RunPurge(purgeLimit, committedIds)
		assert.NoError(t, err)

		// THEN: undo ログが破棄されている
		assert.Nil(t, h.UndoLog().GetRecords(trx2))

		// レコードは残っている (UPDATE なので物理削除されない)
		rv := access.NewReadView(0, nil, ^uint64(0))
		vr := access.NewVersionReader(nil)
		iter, err := tbl.Search(h.BufferPool, rv, vr, access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "Bob", string(record[1]))
	})
}

func TestUndoLog(t *testing.T) {
	t.Run("UndoManager が返される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()
		h := Init()

		// WHEN
		undoLog := h.UndoLog()

		// THEN
		assert.NotNil(t, undoLog)
	})
}

package handler

import (
	"minesql/internal/storage/access"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/page"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	t.Run("グローバル Handler を初期化できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		Init()

		// THEN
		assert.NotNil(t, hdl)
	})

	t.Run("複数回初期化しても同じインスタンスが返される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		handler1 := Init()
		handler2 := Init()

		// THEN
		assert.Same(t, handler1, handler2)
	})
}

func TestGet(t *testing.T) {
	t.Run("初期化後にグローバル Handler を取得できる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()
		Init()

		// WHEN
		h := Get()

		// THEN
		assert.NotNil(t, h)
		assert.NotNil(t, h.BufferPool)
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

func TestShutdown(t *testing.T) {
	t.Run("テーブルが存在する状態で Shutdown がエラーなく完了する", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		// テーブルを作成してデータを挿入
		fileId, err := h.Catalog.AllocateFileId(h.BufferPool)
		assert.NoError(t, err)
		err = h.RegisterDmToBp(fileId, "users")
		assert.NoError(t, err)

		metaPageId, err := h.BufferPool.AllocatePageId(fileId)
		assert.NoError(t, err)
		tbl := access.NewTable("users", metaPageId, 1, nil, nil, nil)
		err = tbl.Create(h.BufferPool)
		assert.NoError(t, err)

		cols := []*dictionary.ColumnMeta{
			dictionary.NewColumnMeta(fileId, "id", 0, dictionary.ColumnTypeString),
		}
		tblMeta := dictionary.NewTableMeta(fileId, "users", 1, 1, cols, nil, metaPageId)
		err = h.Catalog.Insert(h.BufferPool, tblMeta)
		assert.NoError(t, err)

		err = tbl.Insert(h.BufferPool, 0, lock.NewManager(5000), [][]byte{[]byte("1")})
		assert.NoError(t, err)

		// WHEN
		err = h.Shutdown()

		// THEN
		assert.NoError(t, err)
	})
}

func TestPageCleanerLifecycle(t *testing.T) {
	t.Run("Init でページクリーナーが開始され Shutdown で停止する", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		h := Init()

		// THEN: ページクリーナーが開始されている
		assert.NotNil(t, h.pageCleaner)

		// Shutdown でパニックせずに停止する
		err := h.Shutdown()
		assert.NoError(t, err)
	})

	t.Run("Shutdown 後に再度 Init してもページクリーナーが正常に動作する", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		h1 := Init()
		err := h1.Shutdown()
		assert.NoError(t, err)

		// WHEN
		Reset()
		h2 := Init()

		// THEN
		assert.NotNil(t, h2.pageCleaner)
		err = h2.Shutdown()
		assert.NoError(t, err)
	})
}

func TestPageCleanerFlushesDirtyPages(t *testing.T) {
	t.Run("ダーティーページ率が閾値を超えるとページクリーナーがフラッシュする", func(t *testing.T) {
		// GIVEN: バッファプールサイズ 10、ダーティーページ率閾値 1% (ほぼ必ず発動)
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		t.Setenv("MINESQL_MAX_DIRTY_PAGES_PCT", "1")
		Reset()
		h := Init()

		setupTable := func(t *testing.T, h *Handler) {
			t.Helper()
			err := h.CreateTable("users", 1, nil, []CreateColumnParam{
				{Name: "id", Type: ColumnTypeString},
				{Name: "name", Type: ColumnTypeString},
			})
			assert.NoError(t, err)
		}
		setupTable(t, h)

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		// データを挿入してダーティーページを作る
		trxId := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trxId, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		err = h.CommitTrx(trxId)
		assert.NoError(t, err)

		// フラッシュリストにダーティーページがある状態
		flushListBefore := h.BufferPool.FlushListSize()
		assert.Greater(t, flushListBefore, 0)

		// WHEN: ページクリーナーが動くのを待つ (デフォルト 1 秒間隔)
		time.Sleep(2 * time.Second)

		// THEN: ページクリーナーによってフラッシュリストが縮小している
		flushListAfter := h.BufferPool.FlushListSize()
		assert.Less(t, flushListAfter, flushListBefore)

		err = h.Shutdown()
		assert.NoError(t, err)
	})
}

func TestCrashRecovery(t *testing.T) {
	// テーブル作成とデータ挿入のヘルパー
	setupTable := func(t *testing.T, h *Handler) {
		t.Helper()
		err := h.CreateTable("users", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		})
		assert.NoError(t, err)
	}

	t.Run("コミット済みの変更がクラッシュ後に復元される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		setupTable(t, h)

		// テーブル構造をディスクに永続化 (CreateTable は REDO 記録されないため)
		err := h.BufferPool.FlushAllPages()
		assert.NoError(t, err)
		err = h.redoLog.Reset()
		assert.NoError(t, err)

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		trxId := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trxId, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		err = h.CommitTrx(trxId)
		assert.NoError(t, err)

		// WHEN: Shutdown を呼ばずに再初期化 (クラッシュをシミュレーション)
		Reset()
		h2 := Init()

		// THEN: コミット済みデータが復元されている
		tbl2, err := h2.GetTable("users")
		assert.NoError(t, err)

		iter, err := tbl2.Search(h2.BufferPool, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("1"), record[0])
		assert.Equal(t, []byte("Alice"), record[1])

		err = h2.Shutdown()
		assert.NoError(t, err)
	})

	t.Run("未コミットの変更がクラッシュ後にロールバックされる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		setupTable(t, h)
		err := h.BufferPool.FlushAllPages()
		assert.NoError(t, err)
		err = h.redoLog.Reset()
		assert.NoError(t, err)

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		trxId := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trxId, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		// COMMIT しない

		// REDO ログをフラッシュ (ページ変更は記録されるが COMMIT レコードはない)
		err = h.redoLog.Flush()
		assert.NoError(t, err)

		// WHEN: Shutdown を呼ばずに再初期化 (クラッシュをシミュレーション)
		Reset()
		h2 := Init()

		// THEN: 未コミットデータがロールバックされ、テーブルが空
		tbl2, err := h2.GetTable("users")
		assert.NoError(t, err)

		iter, err := tbl2.Search(h2.BufferPool, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok) // レコードが存在しない

		err = h2.Shutdown()
		assert.NoError(t, err)
	})

	t.Run("正常終了後はリカバリが実行されない", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		setupTable(t, h)
		err := h.BufferPool.FlushAllPages()
		assert.NoError(t, err)
		err = h.redoLog.Reset()
		assert.NoError(t, err)

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		trxId := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trxId, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		err = h.CommitTrx(trxId)
		assert.NoError(t, err)

		// 正常終了
		err = h.Shutdown()
		assert.NoError(t, err)

		// WHEN: 再初期化
		Reset()
		h2 := Init()

		// THEN: データが正常に存在する
		tbl2, err := h2.GetTable("users")
		assert.NoError(t, err)

		iter, err := tbl2.Search(h2.BufferPool, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("1"), record[0])

		err = h2.Shutdown()
		assert.NoError(t, err)
	})

	t.Run("コミット済みと未コミットが混在する場合、未コミットのみロールバックされる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		setupTable(t, h)
		err := h.BufferPool.FlushAllPages()
		assert.NoError(t, err)
		err = h.redoLog.Reset()
		assert.NoError(t, err)

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		// trx1: Insert + Commit
		trx1 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx1, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		err = h.CommitTrx(trx1)
		assert.NoError(t, err)

		// ダーティーページをフラッシュしてクリーンにする (trx2 の変更が REDO 記録されるようにする)
		// Reset() は呼ばない (LSN カウンターを維持するため)
		err = h.BufferPool.FlushAllPages()
		assert.NoError(t, err)

		// trx2: Insert (未コミット)
		trx2 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx2, h.LockMgr, [][]byte{[]byte("2"), []byte("Bob")})
		assert.NoError(t, err)

		err = h.redoLog.Flush()
		assert.NoError(t, err)

		// WHEN: Shutdown を呼ばずに再初期化
		Reset()
		h2 := Init()

		// THEN: trx1 のデータは残り、trx2 のデータはロールバックされている
		tbl2, err := h2.GetTable("users")
		assert.NoError(t, err)

		iter, err := tbl2.Search(h2.BufferPool, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("1"), record[0])
		assert.Equal(t, []byte("Alice"), record[1])

		_, ok, err = iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok) // trx2 のデータは存在しない

		err = h2.Shutdown()
		assert.NoError(t, err)
	})

	t.Run("未コミットの UPDATE がクラッシュ後にロールバックされる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "100")
		Reset()
		h := Init()

		setupTable(t, h)
		err := h.BufferPool.FlushAllPages()
		assert.NoError(t, err)
		err = h.redoLog.Reset()
		assert.NoError(t, err)

		tbl, err := h.GetTable("users")
		assert.NoError(t, err)

		// Insert + Commit (ベースデータ)
		trx1 := h.BeginTrx()
		err = tbl.Insert(h.BufferPool, trx1, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		err = h.CommitTrx(trx1)
		assert.NoError(t, err)

		// ダーティーページをフラッシュしてクリーンにする
		err = h.BufferPool.FlushAllPages()
		assert.NoError(t, err)

		// UpdateInplace (未コミット)
		trx2 := h.BeginTrx()
		err = tbl.UpdateInplace(h.BufferPool, trx2, h.LockMgr,
			[][]byte{[]byte("1"), []byte("Alice")},
			[][]byte{[]byte("1"), []byte("Updated")},
		)
		assert.NoError(t, err)

		err = h.redoLog.Flush()
		assert.NoError(t, err)

		// WHEN: Shutdown を呼ばずに再初期化
		Reset()
		h2 := Init()

		// THEN: UPDATE がロールバックされ、元のデータに戻っている
		tbl2, err := h2.GetTable("users")
		assert.NoError(t, err)

		iter, err := tbl2.Search(h2.BufferPool, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("Alice"), record[1]) // "Updated" ではなく "Alice"

		err = h2.Shutdown()
		assert.NoError(t, err)
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
		h := Get()

		fileId := page.FileId(1)
		tableName := "users"

		// WHEN
		err := h.RegisterDmToBp(fileId, tableName)

		// THEN
		assert.NoError(t, err)

		dm, err := h.BufferPool.GetDisk(fileId)
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
		h := Get()

		fileId := page.FileId(1)
		tableName := "users"

		// WHEN
		err1 := h.RegisterDmToBp(fileId, tableName)
		err2 := h.RegisterDmToBp(fileId, tableName)

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)

		dm, err := h.BufferPool.GetDisk(fileId)
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
		h := Init()

		// THEN
		assert.NotNil(t, h)
		assert.NotNil(t, h.Catalog)
		assert.Equal(t, page.FileId(2), h.Catalog.NextFileId)
	})

	t.Run("カタログファイルが既に存在する場合、既存のカタログが開かれる", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// 最初の初期化でカタログを作成
		handler1 := Init()

		// FileId を採番してディスクに保存
		_, err := handler1.Catalog.AllocateFileId(handler1.BufferPool)
		assert.NoError(t, err)
		_, err = handler1.Catalog.AllocateFileId(handler1.BufferPool)
		assert.NoError(t, err)

		// ダーティーページをディスクにフラッシュ
		err = handler1.BufferPool.FlushAllPages()
		assert.NoError(t, err)

		// Handler をリセット
		Reset()

		// WHEN: 同じディレクトリで再初期化
		handler2 := Init()

		// THEN
		assert.NotNil(t, handler2.Catalog)
		assert.Equal(t, page.FileId(4), handler2.Catalog.NextFileId)
	})

	t.Run("カタログの Disk が BufferPool に登録される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		t.Setenv("MINESQL_DATA_DIR", tmpdir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		h := Init()

		// THEN
		dm, err := h.BufferPool.GetDisk(page.FileId(0))
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
		tbl := access.NewTable("users", metaPageId, 1, nil, nil, nil)
		err = tbl.Create(bp)
		assert.NoError(t, err)

		cols := []*dictionary.ColumnMeta{
			dictionary.NewColumnMeta(fileId, "id", 0, dictionary.ColumnTypeString),
		}
		tblMeta := dictionary.NewTableMeta(fileId, "users", 1, 1, cols, nil, metaPageId)
		err = sm1.Catalog.Insert(bp, tblMeta)
		assert.NoError(t, err)

		err = bp.FlushAllPages()
		assert.NoError(t, err)

		Reset()

		// WHEN: 同じディレクトリで再初期化
		sm2 := Init()

		// THEN: テーブルの Disk が登録されている
		dm, err := sm2.BufferPool.GetDisk(fileId)
		assert.NoError(t, err)
		assert.NotNil(t, dm)

		// カタログからテーブル情報も取得できる
		tableMeta, ok := sm2.Catalog.GetTableMetaByName("users")
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
		h := Init()

		// THEN: 新しいカタログが作成され、NextFileId は 1
		assert.NotNil(t, h)
		assert.NotNil(t, h.Catalog)
		assert.Equal(t, page.FileId(2), h.Catalog.NextFileId)
	})

	t.Run("データディレクトリが存在しない場合、自動作成される", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		nestedDir := filepath.Join(tmpdir, "nested", "data")
		t.Setenv("MINESQL_DATA_DIR", nestedDir)
		t.Setenv("MINESQL_BUFFER_SIZE", "10")
		Reset()

		// WHEN
		h := Init()

		// THEN: ディレクトリが作成され、初期化が完了している
		assert.NotNil(t, h)
		_, err := os.Stat(nestedDir)
		assert.NoError(t, err)
	})
}

func TestFindMaxTrxId(t *testing.T) {
	t.Run("テーブルにレコードがある場合、最大の trxId が返される", func(t *testing.T) {
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

		// trxId=2 で INSERT
		err = tbl.Insert(h.BufferPool, 2, lock.NewManager(5000), [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)

		// WHEN
		maxTrxId, err := findMaxTrxId(h.BufferPool, h.Catalog, h.undoLog, h.redoLog)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(2), maxTrxId)
	})

	t.Run("テーブルが空の場合、0 が返される", func(t *testing.T) {
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

		// WHEN
		maxTrxId, err := findMaxTrxId(h.BufferPool, h.Catalog, h.undoLog, h.redoLog)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(0), maxTrxId)
	})

	t.Run("複数テーブルの中で最大の trxId が返される", func(t *testing.T) {
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
		err = h.CreateTable("orders", 1, nil, []CreateColumnParam{
			{Name: "id", Type: ColumnTypeString},
		})
		assert.NoError(t, err)

		tblUsers, err := h.GetTable("users")
		assert.NoError(t, err)
		tblOrders, err := h.GetTable("orders")
		assert.NoError(t, err)

		err = tblUsers.Insert(h.BufferPool, 3, lock.NewManager(5000), [][]byte{[]byte("1")})
		assert.NoError(t, err)
		err = tblOrders.Insert(h.BufferPool, 5, lock.NewManager(5000), [][]byte{[]byte("100")})
		assert.NoError(t, err)

		// WHEN
		maxTrxId, err := findMaxTrxId(h.BufferPool, h.Catalog, h.undoLog, h.redoLog)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, lock.TrxId(5), maxTrxId)
	})
}

func TestNextTrxIdRecovery(t *testing.T) {
	t.Run("再起動後の nextTrxId が既存レコードの trxId より大きい", func(t *testing.T) {
		// GIVEN: テーブル作成 → INSERT → Shutdown → 再初期化
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

		// BEGIN → INSERT → COMMIT
		trxId := h.BeginTrx()
		tbl, err := h.GetTable("users")
		assert.NoError(t, err)
		err = tbl.Insert(h.BufferPool, trxId, h.LockMgr, [][]byte{[]byte("1"), []byte("Alice")})
		assert.NoError(t, err)
		err = h.CommitTrx(trxId)
		assert.NoError(t, err)

		// Shutdown
		err = h.Shutdown()
		assert.NoError(t, err)

		// WHEN: 再初期化 (サーバー再起動相当)
		Reset()
		h2 := Init()

		// 再起動後に BEGIN → SELECT
		trxId2 := h2.BeginTrx()
		rv := h2.CreateReadView(trxId2)

		// THEN: INSERT 時の trxId で書かれたレコードが可視
		assert.True(t, rv.IsVisible(trxId), "再起動前にコミットされた trxId は可視であるべき")
		assert.Greater(t, rv.MLowLimitId, trxId, "MLowLimitId は過去の trxId より大きいべき")
	})
}

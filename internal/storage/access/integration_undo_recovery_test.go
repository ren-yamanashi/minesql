package access

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/ncw/directio"
	"github.com/ren-yamanashi/minesql/internal/storage/btree"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationUndoRecoveryPurge(t *testing.T) {
	t.Run("再起動 → Recovery → Purge で再起動前の deleteMark レコードが物理削除される", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		flushBaseline(t, env)

		trx1 := env.trxMgr.Begin()
		err := table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		record := currentReadFirst(t, table, trx2)
		assert.NoError(t, table.SoftDelete(trx2, record))
		assert.NoError(t, env.trxMgr.Commit(trx2))
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		assert.NoError(t, r.Execute())

		table2, err := NewTable(env2.bp, env2.ct, env2.undoLog, env2.lockMgr, env2.redoLog, "users")
		assert.NoError(t, err)
		p := NewPurge(env2.trxMgr)
		assert.NoError(t, p.purge())

		// THEN
		mtr := buffer.NewMtr(env2.bp)
		defer mtr.UnpinAll()
		iter, err := table2.primaryIndex.tree.Search(mtr, btree.SearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Get()
		assert.NoError(t, err)
		assert.False(t, ok, "Purge により再起動前の deleteMark レコードが物理削除されている")
	})

	t.Run("再起動時に DELETE Undo を持つ trxId が InactiveTrxIds に再構築される", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		flushBaseline(t, env)

		trx1 := env.trxMgr.Begin()
		err := table.Insert(
			trx1,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		assert.NoError(t, env.trxMgr.Commit(trx1))

		trx2 := env.trxMgr.Begin()
		record := currentReadFirst(t, table, trx2)
		assert.NoError(t, table.SoftDelete(trx2, record))
		assert.NoError(t, env.trxMgr.Commit(trx2))
		deleteTrxId := trx2.trxId
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		assert.NoError(t, r.Execute())

		// THEN
		ids := env2.trxMgr.InactiveTrxIds()
		assert.Contains(t, ids, deleteTrxId, "DELETE Undo を持つ trxId は再構築された InactiveTrxIds に含まれる")
		assert.NotContains(t, ids, trx1.trxId, "INSERT のみの trxId は History List から外れているため含まれない")
	})

	t.Run("Insert のみのコミット済みトランザクションは InactiveTrxIds に含まれない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		flushBaseline(t, env)
		trx := env.trxMgr.Begin()
		err := table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		)
		assert.NoError(t, err)
		assert.NoError(t, env.trxMgr.Commit(trx))
		assert.NoError(t, env.redoLog.Flush())

		// WHEN
		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		assert.NoError(t, r.Execute())

		// THEN
		ids := env2.trxMgr.InactiveTrxIds()
		assert.NotContains(t, ids, trx.trxId, "INSERT のみのトランザクションは History List から外れる")
	})
}

func TestIntegrationIncompleteMtrFlushSkip(t *testing.T) {
	t.Run("変更ありの X-latch を保持中のページは FlushAllPages で disk に書き出されない", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		trx := env.trxMgr.Begin()
		require.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		))
		require.NoError(t, env.trxMgr.Commit(trx))
		flushBaseline(t, env)

		metaPageId := table.primaryIndex.tree.MetaPageId()
		baselineBytes := readPageBytesFromDisk(t, metaPageId.FileId(), "users.db", metaPageId.PageNumber())

		// WHEN
		mtr := buffer.NewWriteMtr(env.bp, lock.TrxId(99999), env.redoLog)
		bufPage, err := mtr.PageForWrite(metaPageId)
		require.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{0xFF, 0xFF, 0xFF, 0xFF})
		require.NoError(t, env.bp.FlushAllPages())

		// THEN
		afterBytes := readPageBytesFromDisk(t, metaPageId.FileId(), "users.db", metaPageId.PageNumber())
		assert.Equal(t, baselineBytes, afterBytes, "Pin 中のページは flush 対象から除外されディスク上のバイトが変化しない")

		mtr.UnpinAll()
	})
}

func TestIntegrationIncompleteMtrRecovery(t *testing.T) {
	t.Run("MtrEnd 不在の変更は Recovery で破棄されページが変更前バイトに戻る", func(t *testing.T) {
		// GIVEN
		env := setupIntegrationEnv(t)
		table := createUsersTable(t, env)
		trx := env.trxMgr.Begin()
		require.NoError(t, table.Insert(
			trx,
			[]string{"id", "name", "email"},
			[]string{"1", "Alice", "alice@example.com"},
		))
		require.NoError(t, env.trxMgr.Commit(trx))
		flushBaseline(t, env)

		metaPageId := table.primaryIndex.tree.MetaPageId()
		baselineBytes := readPageBytesFromDisk(t, metaPageId.FileId(), "users.db", metaPageId.PageNumber())

		// WHEN
		mtr := buffer.NewWriteMtr(env.bp, lock.TrxId(99999), env.redoLog)
		bufPage, err := mtr.PageForWrite(metaPageId)
		require.NoError(t, err)
		bufPage.WriteBodyAt(0, []byte{0xFF, 0xFF, 0xFF, 0xFF})
		mtr.UnpinAll()
		require.NoError(t, env.redoLog.Flush())

		env2 := crashAndRecover(t, env, []string{"users"})
		r := NewRecovery(env2.redoLog, env2.bp, env2.trxMgr, env2.ct.UndoLogFileId(), env2.ddlMgr)
		require.NoError(t, r.Execute())

		// THEN
		recoveredBytes := readPageBytesFromDisk(t, metaPageId.FileId(), "users.db", metaPageId.PageNumber())
		assert.Equal(t, baselineBytes, recoveredBytes, "MtrEnd 不在の PageWrite は Recovery で破棄されベースライン状態に復元される")
	})
}

// readPageBytesFromDisk は HeapFile を別ハンドルで open し、 指定ページのディスク上の生バイトを返す
//   - bufferPool のキャッシュを経由しないため、 ディスクへの書き出しが行われたかを直接検証できる
//   - direct I/O のアライメント制約に従い directio.AlignedBlock で読み込みバッファを確保する
func readPageBytesFromDisk(t *testing.T, fileId page.FileId, filename string, pageNumber page.PageNumber) []byte {
	t.Helper()
	hf, err := file.NewHeapFile(fileId, filepath.Join(config.BaseDir, filename))
	if err != nil {
		t.Fatalf("HeapFile の再オープンに失敗: %v", err)
	}
	defer func() { _ = hf.Close() }()
	buf := directio.AlignedBlock(page.Size)
	if err := hf.Read(pageNumber, buf); err != nil {
		t.Fatalf("HeapFile.Read に失敗: %v", err)
	}
	return slices.Clone(buf)
}

package recovery

import (
	"encoding/binary"
	"minesql/internal/storage/access"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/dictionary"
	"minesql/internal/storage/file"
	"minesql/internal/storage/lock"
	"minesql/internal/storage/log"
	"minesql/internal/storage/page"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNeedsRecovery(t *testing.T) {
	t.Run("REDO ログが空の場合はリカバリ不要", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(10, rl)

		rec := NewRecovery(rl, bp, nil, page.FileId(0))

		// WHEN
		needs, err := rec.NeedsRecovery()

		// THEN
		assert.NoError(t, err)
		assert.False(t, needs)
	})

	t.Run("REDO ログにレコードがある場合はリカバリ必要", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(10, rl)

		rl.AppendPageCopy(1, page.NewPageId(1, 0), make([]byte, page.PageSize))
		err = rl.Flush()
		assert.NoError(t, err)

		rec := NewRecovery(rl, bp, nil, page.FileId(0))

		// WHEN
		needs, err := rec.NeedsRecovery()

		// THEN
		assert.NoError(t, err)
		assert.True(t, needs)
	})
}

func TestRedoApply(t *testing.T) {
	t.Run("REDO ログからページが復元される", func(t *testing.T) {
		// GIVEN: ディスク上のページとそれを変更した REDO レコードを用意
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(10, rl)

		fileId := page.FileId(1)
		disk, err := file.NewDisk(fileId, filepath.Join(tmpdir, "test.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(fileId, disk)

		// ページを作成して初期データを書き込み
		pageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		err = bp.AddPage(pageId)
		assert.NoError(t, err)
		writeData, err := bp.GetWritePageData(pageId)
		assert.NoError(t, err)
		writeData[page.PageHeaderSize] = 0x00 // 初期値
		err = bp.FlushAllPages()
		assert.NoError(t, err)
		err = rl.Reset()
		assert.NoError(t, err)

		// 変更後のページデータを含む REDO レコードを記録
		modifiedPage := make([]byte, page.PageSize)
		modifiedPage[page.PageHeaderSize] = 0xFF                           // 変更後の値
		binary.BigEndian.PutUint32(modifiedPage[0:page.PageHeaderSize], 1) // Page LSN = 1
		rl.AppendPageCopy(1, pageId, modifiedPage)
		err = rl.Flush()
		assert.NoError(t, err)

		// ディスクからページを再読み込みするためバッファプールを再作成
		bp2 := buffer.NewBufferPool(10, nil)
		bp2.RegisterDisk(fileId, disk)
		rec := NewRecovery(rl, bp2, nil, page.FileId(0))

		// WHEN
		err = rec.Run()
		assert.NoError(t, err)

		// THEN: ページが REDO レコードの内容で復元されている
		restoredData, err := bp2.GetReadPageData(pageId)
		assert.NoError(t, err)
		assert.Equal(t, byte(0xFF), restoredData[page.PageHeaderSize])
	})

	t.Run("Page LSN が REDO レコードの LSN 以上の場合はスキップ", func(t *testing.T) {
		// GIVEN
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(10, rl)

		fileId := page.FileId(1)
		disk, err := file.NewDisk(fileId, filepath.Join(tmpdir, "test.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(fileId, disk)

		// Page LSN = 10 のページを作成
		pageId, err := bp.AllocatePageId(fileId)
		assert.NoError(t, err)
		err = bp.AddPage(pageId)
		assert.NoError(t, err)
		writeData, err := bp.GetWritePageData(pageId)
		assert.NoError(t, err)
		binary.BigEndian.PutUint32(writeData[0:page.PageHeaderSize], 10) // Page LSN = 10
		writeData[page.PageHeaderSize] = 0xAA                            // 元の値
		err = bp.FlushAllPages()
		assert.NoError(t, err)
		err = rl.Reset()
		assert.NoError(t, err)

		// LSN = 5 の REDO レコード (Page LSN 10 より古い)
		modifiedPage := make([]byte, page.PageSize)
		modifiedPage[page.PageHeaderSize] = 0xFF
		binary.BigEndian.PutUint32(modifiedPage[0:page.PageHeaderSize], 5) // LSN = 5
		rl.AppendPageCopy(1, pageId, modifiedPage)
		err = rl.Flush()
		assert.NoError(t, err)

		// WHEN
		bp2 := buffer.NewBufferPool(10, nil)
		bp2.RegisterDisk(fileId, disk)
		rec := NewRecovery(rl, bp2, nil, page.FileId(0))
		err = rec.Run()
		assert.NoError(t, err)

		// THEN: ページは元の値のまま (REDO がスキップされた)
		restoredData, err := bp2.GetReadPageData(pageId)
		assert.NoError(t, err)
		assert.Equal(t, byte(0xAA), restoredData[page.PageHeaderSize])
	})
}

func TestUndoRollback(t *testing.T) {
	t.Run("未コミットトランザクションの INSERT がロールバックされる", func(t *testing.T) {
		// GIVEN: テーブル作成 → INSERT → COMMIT なしで REDO ログにレコードを残す
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(100, rl)

		// カタログ用ディスク
		catalogDisk, err := file.NewDisk(page.FileId(0), filepath.Join(tmpdir, "minesql.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(page.FileId(0), catalogDisk)
		catalog, err := dictionary.CreateCatalog(bp)
		assert.NoError(t, err)

		// テーブル用ディスク
		tableFileId, err := catalog.AllocateFileId(bp)
		assert.NoError(t, err)
		tableDisk, err := file.NewDisk(tableFileId, filepath.Join(tmpdir, "users.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(tableFileId, tableDisk)

		// UNDO 用ディスク
		undoFileId := catalog.UndoFileId
		undoDisk, err := file.NewDisk(undoFileId, filepath.Join(tmpdir, "undo.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(undoFileId, undoDisk)
		undoLog, err := access.NewUndoManager(bp, rl, undoFileId)
		assert.NoError(t, err)

		// テーブル作成
		metaPageId, err := bp.AllocatePageId(tableFileId)
		assert.NoError(t, err)
		table := access.NewTable("users", metaPageId, 1, nil, undoLog, rl)
		err = table.Create(bp)
		assert.NoError(t, err)

		// カタログにテーブルメタを登録
		colMeta := []*dictionary.ColumnMeta{
			dictionary.NewColumnMeta(tableFileId, "id", 0, dictionary.ColumnTypeString),
			dictionary.NewColumnMeta(tableFileId, "name", 1, dictionary.ColumnTypeString),
		}
		tblMeta := dictionary.NewTableMeta(tableFileId, "users", 2, 1, colMeta, nil, metaPageId)
		err = catalog.Insert(bp, tblMeta)
		assert.NoError(t, err)

		// フラッシュしてクリーンな状態にする
		err = bp.FlushAllPages()
		assert.NoError(t, err)
		err = rl.Reset()
		assert.NoError(t, err)

		// INSERT (COMMIT なし)
		var trxId uint64 = 1
		lockMgr := lock.NewManager(5000)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		// REDO ログをフラッシュ (COMMIT レコードは書かない)
		err = rl.Flush()
		assert.NoError(t, err)

		// WHEN: リカバリを実行
		// バッファプールを再作成してディスクから読み直す
		bp2 := buffer.NewBufferPool(100, nil)
		bp2.RegisterDisk(page.FileId(0), catalogDisk)
		bp2.RegisterDisk(tableFileId, tableDisk)
		bp2.RegisterDisk(undoFileId, undoDisk)
		catalog2, err := dictionary.NewCatalog(bp2)
		assert.NoError(t, err)

		rec := NewRecovery(rl, bp2, catalog2, undoFileId)
		err = rec.Run()
		assert.NoError(t, err)

		// THEN: INSERT がロールバックされ、テーブルが空になっている
		table2 := access.NewTable("users", metaPageId, 1, nil, nil, nil)
		iter, err := table2.Search(bp2, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok) // レコードが存在しない
	})

	t.Run("コミット済みトランザクションはロールバックされない", func(t *testing.T) {
		// GIVEN: INSERT → COMMIT の REDO ログを作成
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(100, rl)

		// カタログ用ディスク
		catalogDisk, err := file.NewDisk(page.FileId(0), filepath.Join(tmpdir, "minesql.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(page.FileId(0), catalogDisk)
		catalog, err := dictionary.CreateCatalog(bp)
		assert.NoError(t, err)

		// テーブル用ディスク
		tableFileId, err := catalog.AllocateFileId(bp)
		assert.NoError(t, err)
		tableDisk, err := file.NewDisk(tableFileId, filepath.Join(tmpdir, "users.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(tableFileId, tableDisk)

		// UNDO 用ディスク
		undoFileId := catalog.UndoFileId
		undoDisk, err := file.NewDisk(undoFileId, filepath.Join(tmpdir, "undo.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(undoFileId, undoDisk)
		undoLog, err := access.NewUndoManager(bp, rl, undoFileId)
		assert.NoError(t, err)

		// テーブル作成
		metaPageId, err := bp.AllocatePageId(tableFileId)
		assert.NoError(t, err)
		table := access.NewTable("users", metaPageId, 1, nil, undoLog, rl)
		err = table.Create(bp)
		assert.NoError(t, err)

		// カタログにテーブルメタを登録
		colMeta := []*dictionary.ColumnMeta{
			dictionary.NewColumnMeta(tableFileId, "id", 0, dictionary.ColumnTypeString),
			dictionary.NewColumnMeta(tableFileId, "name", 1, dictionary.ColumnTypeString),
		}
		tblMeta := dictionary.NewTableMeta(tableFileId, "users", 2, 1, colMeta, nil, metaPageId)
		err = catalog.Insert(bp, tblMeta)
		assert.NoError(t, err)

		// フラッシュしてクリーンな状態にする
		err = bp.FlushAllPages()
		assert.NoError(t, err)
		err = rl.Reset()
		assert.NoError(t, err)

		// INSERT → COMMIT
		var trxId uint64 = 1
		lockMgr := lock.NewManager(5000)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)
		manager := access.NewTrxManager(undoLog, lockMgr, rl)
		manager.Begin() // trxId=1
		err = manager.Commit(trxId)
		assert.NoError(t, err)

		// WHEN: リカバリを実行
		bp2 := buffer.NewBufferPool(100, nil)
		bp2.RegisterDisk(page.FileId(0), catalogDisk)
		bp2.RegisterDisk(tableFileId, tableDisk)
		bp2.RegisterDisk(undoFileId, undoDisk)
		catalog2, err := dictionary.NewCatalog(bp2)
		assert.NoError(t, err)

		rec := NewRecovery(rl, bp2, catalog2, undoFileId)
		err = rec.Run()
		assert.NoError(t, err)

		// THEN: コミット済みの INSERT はロールバックされず、レコードが存在する
		table2 := access.NewTable("users", metaPageId, 1, nil, nil, nil)
		iter, err := table2.Search(bp2, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		record, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte("a"), record[0])
	})

	t.Run("セカンダリインデックス付きテーブルの未コミット INSERT がロールバックされる", func(t *testing.T) {
		// GIVEN: セカンダリインデックス付きテーブル作成 → INSERT → COMMIT なし
		tmpdir := t.TempDir()
		rl, err := log.NewRedoLog(tmpdir)
		assert.NoError(t, err)
		bp := buffer.NewBufferPool(100, rl)

		// カタログ用ディスク
		catalogDisk, err := file.NewDisk(page.FileId(0), filepath.Join(tmpdir, "minesql.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(page.FileId(0), catalogDisk)
		catalog, err := dictionary.CreateCatalog(bp)
		assert.NoError(t, err)

		// テーブル用ディスク
		tableFileId, err := catalog.AllocateFileId(bp)
		assert.NoError(t, err)
		tableDisk, err := file.NewDisk(tableFileId, filepath.Join(tmpdir, "users.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(tableFileId, tableDisk)

		// UNDO 用ディスク
		undoFileId := catalog.UndoFileId
		undoDisk, err := file.NewDisk(undoFileId, filepath.Join(tmpdir, "undo.db"))
		assert.NoError(t, err)
		bp.RegisterDisk(undoFileId, undoDisk)
		undoLog, err := access.NewUndoManager(bp, rl, undoFileId)
		assert.NoError(t, err)

		// セカンダリインデックスの B+Tree を作成
		idxMetaPageId, err := bp.AllocatePageId(tableFileId)
		assert.NoError(t, err)
		si := access.NewSecondaryIndex("idx_name", "name", idxMetaPageId, 1, 1, true)
		err = si.Create(bp)
		assert.NoError(t, err)

		// テーブル作成
		metaPageId, err := bp.AllocatePageId(tableFileId)
		assert.NoError(t, err)
		table := access.NewTable("users", metaPageId, 1, []*access.SecondaryIndex{si}, undoLog, rl)
		err = table.Create(bp)
		assert.NoError(t, err)

		// カタログにテーブルメタを登録
		colMeta := []*dictionary.ColumnMeta{
			dictionary.NewColumnMeta(tableFileId, "id", 0, dictionary.ColumnTypeString),
			dictionary.NewColumnMeta(tableFileId, "name", 1, dictionary.ColumnTypeString),
		}
		idxMeta := []*dictionary.IndexMeta{
			dictionary.NewIndexMeta(tableFileId, "idx_name", "name", dictionary.IndexTypeUnique, si.MetaPageId),
		}
		tblMeta := dictionary.NewTableMeta(tableFileId, "users", 2, 1, colMeta, idxMeta, metaPageId)
		err = catalog.Insert(bp, tblMeta)
		assert.NoError(t, err)

		// フラッシュしてクリーンな状態にする
		err = bp.FlushAllPages()
		assert.NoError(t, err)
		err = rl.Reset()
		assert.NoError(t, err)

		// INSERT (COMMIT なし)
		var trxId uint64 = 1
		lockMgr := lock.NewManager(5000)
		err = table.Insert(bp, trxId, lockMgr, [][]byte{[]byte("a"), []byte("Alice")})
		assert.NoError(t, err)

		err = rl.Flush()
		assert.NoError(t, err)

		// WHEN: リカバリを実行
		bp2 := buffer.NewBufferPool(100, nil)
		bp2.RegisterDisk(page.FileId(0), catalogDisk)
		bp2.RegisterDisk(tableFileId, tableDisk)
		bp2.RegisterDisk(undoFileId, undoDisk)
		catalog2, err := dictionary.NewCatalog(bp2)
		assert.NoError(t, err)

		rec := NewRecovery(rl, bp2, catalog2, undoFileId)
		err = rec.Run()
		assert.NoError(t, err)

		// THEN: テーブルが空になっている
		table2 := access.NewTable("users", metaPageId, 1, []*access.SecondaryIndex{
			access.NewSecondaryIndex("idx_name", "name", si.MetaPageId, 1, 1, true),
		}, nil, nil)
		iter, err := table2.Search(bp2, access.NewReadView(0, nil, ^uint64(0)), access.NewVersionReader(nil), access.RecordSearchModeStart{})
		assert.NoError(t, err)
		_, ok, err := iter.Next()
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

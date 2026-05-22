package redo

import (
	"os"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/config"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewFile(t *testing.T) {
	t.Run("新規ファイルを作成できる", func(t *testing.T) {
		// GIVEN
		setupRedoTestDir(t)

		// WHEN
		f, err := newFile()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, f)
		assert.Equal(t, Lsn(0), f.flushedLsn)
		assert.Equal(t, Lsn(0), f.checkpointLsn)
		_ = f.file.Close()
	})

	t.Run("既存ファイルを開くとヘッダーが読み取られる", func(t *testing.T) {
		// GIVEN
		setupRedoTestDir(t)
		f1, err := newFile()
		assert.NoError(t, err)
		pg := buildTestPage(t)
		records := []Record{{Lsn: Lsn(5), TrxId: 1, Type: RecordTypePageWrite, PageId: page.NewPageId(1, 1), Data: *pg}}
		err = f1.flushRecords(records)
		assert.NoError(t, err)
		_ = f1.file.Close()

		// WHEN
		f2, err := newFile()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(5), f2.flushedLsn)
		_ = f2.file.Close()
	})

	t.Run("既存ファイルから checkpointLsn も復元される", func(t *testing.T) {
		// GIVEN
		setupRedoTestDir(t)
		f1, err := newFile()
		assert.NoError(t, err)
		err = f1.setCheckpointLsn(Lsn(10))
		assert.NoError(t, err)
		_ = f1.file.Close()

		// WHEN
		f2, err := newFile()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(10), f2.checkpointLsn)
		_ = f2.file.Close()
	})
}

func TestFileFlushRecords(t *testing.T) {
	t.Run("レコードをフラッシュすると flushedLsn が更新される", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		pg := buildTestPage(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypePageWrite, PageId: page.NewPageId(1, 1), Data: *pg},
			{Lsn: Lsn(2), TrxId: 1, Type: RecordTypeCommit},
		}

		// WHEN
		err := f.flushRecords(records)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(2), f.flushedLsn)
	})

	t.Run("空のレコードスライスをフラッシュしてもエラーにならない", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)

		// WHEN
		err := f.flushRecords([]Record{})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(0), f.flushedLsn)
	})

	t.Run("複数回フラッシュするとレコードが追記される", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		first := []Record{{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit}}
		_ = f.flushRecords(first)

		second := []Record{{Lsn: Lsn(2), TrxId: 2, Type: RecordTypeCommit}}

		// WHEN
		err := f.flushRecords(second)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(2), f.flushedLsn)
		result, err := f.readRecords(Lsn(0))
		assert.NoError(t, err)
		assert.Len(t, result, 2)
	})
}

func TestFileReadRecords(t *testing.T) {
	t.Run("フラッシュしたレコードを読み取れる", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		pg := buildTestPage(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypePageWrite, PageId: page.NewPageId(1, 1), Data: *pg},
			{Lsn: Lsn(2), TrxId: 1, Type: RecordTypeCommit},
		}
		_ = f.flushRecords(records)

		// WHEN
		result, err := f.readRecords(Lsn(0))

		// THEN
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, Lsn(1), result[0].Lsn)
		assert.Equal(t, Lsn(2), result[1].Lsn)
	})

	t.Run("指定 LSN より大きいレコードだけ返す", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit},
			{Lsn: Lsn(2), TrxId: 2, Type: RecordTypeCommit},
			{Lsn: Lsn(3), TrxId: 3, Type: RecordTypeCommit},
		}
		_ = f.flushRecords(records)

		// WHEN
		result, err := f.readRecords(Lsn(1))

		// THEN
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, Lsn(2), result[0].Lsn)
		assert.Equal(t, Lsn(3), result[1].Lsn)
	})

	t.Run("空のファイルから読み取ると nil を返す", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)

		// WHEN
		result, err := f.readRecords(Lsn(0))

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("全レコードが指定 LSN 以下の場合 nil を返す", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit},
			{Lsn: Lsn(2), TrxId: 2, Type: RecordTypeCommit},
		}
		_ = f.flushRecords(records)

		// WHEN
		result, err := f.readRecords(Lsn(5))

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("ページ変更レコードのデータが正しく読み取れる", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		pg := buildTestPage(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypePageWrite, PageId: page.NewPageId(2, 3), Data: *pg},
		}
		_ = f.flushRecords(records)

		// WHEN
		result, err := f.readRecords(Lsn(0))

		// THEN
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, RecordTypePageWrite, result[0].Type)
		assert.Equal(t, page.NewPageId(2, 3), result[0].PageId)
		assert.Equal(t, pg.ToBytes(), result[0].Data.ToBytes())
	})
}

func TestFileSetCheckpointLsn(t *testing.T) {
	t.Run("checkpointLsn を更新できる", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)

		// WHEN
		err := f.setCheckpointLsn(Lsn(7))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(7), f.checkpointLsn)
	})

	t.Run("更新した checkpointLsn がヘッダーに永続化される", func(t *testing.T) {
		// GIVEN
		setupRedoTestDir(t)
		f1, err := newFile()
		assert.NoError(t, err)
		err = f1.setCheckpointLsn(Lsn(15))
		assert.NoError(t, err)
		_ = f1.file.Close()

		// WHEN
		f2, err := newFile()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(15), f2.checkpointLsn)
		_ = f2.file.Close()
	})
}

func TestFileSize(t *testing.T) {
	t.Run("新規ファイルのサイズはヘッダーサイズと等しい", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)

		// WHEN
		size, err := f.size()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, int64(fileHeaderSize), size)
	})

	t.Run("レコードフラッシュ後にサイズが増加する", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit},
		}
		_ = f.flushRecords(records)

		// WHEN
		size, err := f.size()

		// THEN
		assert.NoError(t, err)
		assert.Greater(t, size, int64(fileHeaderSize))
	})
}

func TestFileClear(t *testing.T) {
	t.Run("クリア後にレコードが空になる", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit},
		}
		_ = f.flushRecords(records)

		// WHEN
		err := f.clear()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(0), f.flushedLsn)
		assert.Equal(t, Lsn(0), f.checkpointLsn)

		result, err := f.readRecords(Lsn(0))
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("checkpointLsn が設定されている状態からクリアすると 0 に戻る", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		_ = f.setCheckpointLsn(Lsn(20))
		records := []Record{{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit}}
		_ = f.flushRecords(records)

		// WHEN
		err := f.clear()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(0), f.flushedLsn)
		assert.Equal(t, Lsn(0), f.checkpointLsn)
	})

	t.Run("クリア後にファイルサイズがヘッダーサイズになる", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		records := []Record{{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit}}
		_ = f.flushRecords(records)

		// WHEN
		_ = f.clear()

		// THEN
		size, err := f.size()
		assert.NoError(t, err)
		assert.Equal(t, int64(fileHeaderSize), size)
	})
}

func TestFileTruncateBefore(t *testing.T) {
	t.Run("指定 LSN 以前のレコードが削除される", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit},
			{Lsn: Lsn(2), TrxId: 2, Type: RecordTypeCommit},
			{Lsn: Lsn(3), TrxId: 3, Type: RecordTypeCommit},
		}
		_ = f.flushRecords(records)

		// WHEN
		err := f.truncateBefore(Lsn(2))

		// THEN
		assert.NoError(t, err)
		result, err := f.readRecords(Lsn(0))
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, Lsn(3), result[0].Lsn)
	})

	t.Run("全レコードの LSN 以上を指定すると全て削除される", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit},
			{Lsn: Lsn(2), TrxId: 2, Type: RecordTypeCommit},
		}
		_ = f.flushRecords(records)

		// WHEN
		err := f.truncateBefore(Lsn(2))

		// THEN
		assert.NoError(t, err)
		result, err := f.readRecords(Lsn(0))
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("空のファイルに対して truncate してもエラーにならない", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)

		// WHEN
		err := f.truncateBefore(Lsn(5))

		// THEN
		assert.NoError(t, err)
	})

	t.Run("LSN 0 を指定すると全レコードが保持される", func(t *testing.T) {
		// GIVEN
		f := setupTestFile(t)
		records := []Record{
			{Lsn: Lsn(1), TrxId: 1, Type: RecordTypeCommit},
			{Lsn: Lsn(2), TrxId: 2, Type: RecordTypeCommit},
		}
		_ = f.flushRecords(records)

		// WHEN
		err := f.truncateBefore(Lsn(0))

		// THEN
		assert.NoError(t, err)
		result, err := f.readRecords(Lsn(0))
		assert.NoError(t, err)
		assert.Len(t, result, 2)
	})
}

func TestFileWriteHeader(t *testing.T) {
	t.Run("flushedLsn と checkpointLsn がヘッダーに書き込まれる", func(t *testing.T) {
		// GIVEN
		setupRedoTestDir(t)
		f1, err := newFile()
		assert.NoError(t, err)
		f1.flushedLsn = Lsn(100)
		f1.checkpointLsn = Lsn(50)
		err = f1.writeHeader()
		assert.NoError(t, err)
		_ = f1.file.Close()

		// WHEN
		f2, err := newFile()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(100), f2.flushedLsn)
		assert.Equal(t, Lsn(50), f2.checkpointLsn)
		_ = f2.file.Close()
	})
}

// setupRedoTestDir は config.BaseDir ディレクトリを作成し、テスト終了時にディレクトリごと削除する
func setupRedoTestDir(t *testing.T) {
	t.Helper()
	_ = os.MkdirAll(config.BaseDir, 0o750)
	t.Cleanup(func() {
		_ = os.RemoveAll(config.BaseDir)
	})
}

// setupTestFile はテスト用の File を作成する
func setupTestFile(t *testing.T) *File {
	t.Helper()
	setupRedoTestDir(t)
	f, err := newFile()
	if err != nil {
		t.Fatalf("File の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = f.file.Close() })
	return f
}

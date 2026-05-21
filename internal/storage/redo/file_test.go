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
		assert.Equal(t, Lsn(0), f.checkPointLsn)
	})

	t.Run("既存ファイルを開くとヘッダーが読み取られる", func(t *testing.T) {
		// GIVEN
		setupRedoTestDir(t)
		f1, err := newFile()
		assert.NoError(t, err)
		// レコードを書き込んで flushedLsn を更新
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
		assert.Equal(t, Lsn(0), f.checkPointLsn)

		result, err := f.readRecords(Lsn(0))
		assert.NoError(t, err)
		assert.Nil(t, result)
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
}

// setupRedoTestDir は config.BaseDir ディレクトリを作成し、テスト終了時に Redo ログファイルを削除する
func setupRedoTestDir(t *testing.T) {
	t.Helper()
	_ = os.MkdirAll(config.BaseDir, 0o750)
	t.Cleanup(func() {
		_ = os.Remove(config.BaseDir + "/" + filename)
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

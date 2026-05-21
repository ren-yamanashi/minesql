package redo

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewBuffer(t *testing.T) {
	t.Run("Buffer を作成できる", func(t *testing.T) {
		// GIVEN
		setupRedoTestDir(t)

		// WHEN
		buf, err := NewBuffer()

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, buf)
	})
}

func TestBufferAppendPageCopy(t *testing.T) {
	t.Run("ページ変更レコードを追加すると LSN を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)

		// WHEN
		lsn := buf.AppendPageCopy(lock.TrxId(1), page.NewPageId(1, 1), *pg)

		// THEN
		assert.Equal(t, Lsn(1), lsn)
	})

	t.Run("連続して追加するとインクリメントされた LSN を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)

		// WHEN
		lsn1 := buf.AppendPageCopy(lock.TrxId(1), page.NewPageId(1, 1), *pg)
		lsn2 := buf.AppendPageCopy(lock.TrxId(1), page.NewPageId(1, 2), *pg)

		// THEN
		assert.Equal(t, Lsn(0), lsn1)
		assert.Equal(t, Lsn(2), lsn2)
	})
}

func TestBufferAppendCommit(t *testing.T) {
	t.Run("COMMIT レコードを追加できる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		lsn := buf.AppendCommit(lock.TrxId(1))

		// THEN
		assert.Equal(t, Lsn(1), lsn)
	})
}

func TestBufferAppendRollback(t *testing.T) {
	t.Run("ROLLBACK レコードを追加できる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		lsn := buf.AppendRollback(lock.TrxId(1))

		// THEN
		assert.Equal(t, Lsn(1), lsn)
	})
}

func TestBufferFlush(t *testing.T) {
	t.Run("バッファのレコードをディスクに書き込む", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		buf.AppendCommit(lock.TrxId(1))

		// WHEN
		err := buf.Flush()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(1), buf.FlushedLsn())
	})

	t.Run("空のバッファをフラッシュしてもエラーにならない", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		err := buf.Flush()

		// THEN
		assert.NoError(t, err)
	})

	t.Run("フラッシュ後にバッファが空になる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		buf.AppendCommit(lock.TrxId(1))

		// WHEN
		_ = buf.Flush()

		// THEN
		assert.Equal(t, 0, buf.Size())
	})
}

func TestBufferReadAll(t *testing.T) {
	t.Run("フラッシュ済みの全レコードを読み取れる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)
		buf.AppendPageCopy(lock.TrxId(1), page.NewPageId(1, 1), *pg)
		buf.AppendCommit(lock.TrxId(1))
		_ = buf.Flush()

		// WHEN
		records, err := buf.ReadAll()

		// THEN
		assert.NoError(t, err)
		assert.Len(t, records, 2)
		assert.Equal(t, Lsn(1), records[0].Lsn)
		assert.Equal(t, Lsn(2), records[1].Lsn)
	})
}

func TestBufferReadFrom(t *testing.T) {
	t.Run("指定 LSN より大きいレコードだけ返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		buf.AppendCommit(lock.TrxId(1)) // LSN=1
		buf.AppendCommit(lock.TrxId(2)) // LSN=2
		buf.AppendCommit(lock.TrxId(3)) // LSN=3
		_ = buf.Flush()

		// WHEN
		records, err := buf.ReadFrom(Lsn(1))

		// THEN
		assert.NoError(t, err)
		assert.Len(t, records, 2)
		assert.Equal(t, Lsn(2), records[0].Lsn)
		assert.Equal(t, Lsn(3), records[1].Lsn)
	})
}

func TestBufferSetCheckpointLsn(t *testing.T) {
	t.Run("チェックポイント LSN を更新できる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		err := buf.SetCheckpointLsn(Lsn(10))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(10), buf.CheckpointLsn())
	})
}

func TestBufferCheckpointLsn(t *testing.T) {
	t.Run("初期値は 0 を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		lsn := buf.CheckpointLsn()

		// THEN
		assert.Equal(t, Lsn(0), lsn)
	})
}

func TestBufferFlushedLsn(t *testing.T) {
	t.Run("フラッシュ前は 0 を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		lsn := buf.FlushedLsn()

		// THEN
		assert.Equal(t, Lsn(1), lsn)
	})

	t.Run("フラッシュ後は最後のレコードの LSN を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		buf.AppendCommit(lock.TrxId(1)) // LSN=0
		buf.AppendCommit(lock.TrxId(2)) // LSN=1
		_ = buf.Flush()

		// WHEN
		lsn := buf.FlushedLsn()

		// THEN
		assert.Equal(t, Lsn(1), lsn)
	})
}

func TestBufferClear(t *testing.T) {
	t.Run("クリア後にレコードが空になる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		buf.AppendCommit(lock.TrxId(1))
		_ = buf.Flush()

		// WHEN
		err := buf.Clear()

		// THEN
		assert.NoError(t, err)
		records, err := buf.ReadAll()
		assert.NoError(t, err)
		assert.Nil(t, records)
	})
}

func TestBufferTruncateBefore(t *testing.T) {
	t.Run("指定 LSN 以前のレコードが削除される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		buf.AppendCommit(lock.TrxId(1)) // LSN=0
		buf.AppendCommit(lock.TrxId(2)) // LSN=1
		buf.AppendCommit(lock.TrxId(3)) // LSN=2
		_ = buf.Flush()

		// WHEN
		err := buf.TruncateBefore(Lsn(1))

		// THEN
		assert.NoError(t, err)
		records, err := buf.ReadAll()
		assert.NoError(t, err)
		assert.Len(t, records, 1)
		assert.Equal(t, Lsn(2), records[0].Lsn)
	})
}

func TestBufferSize(t *testing.T) {
	t.Run("空のバッファは 0 を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		size := buf.Size()

		// THEN
		assert.Equal(t, 0, size)
	})

	t.Run("COMMIT レコード追加後はヘッダーサイズのみ", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		buf.AppendCommit(lock.TrxId(1))

		// WHEN
		size := buf.Size()

		// THEN
		assert.Equal(t, recordHeaderSize, size)
	})

	t.Run("ページ変更レコード追加後はヘッダー + ページサイズ", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)
		buf.AppendPageCopy(lock.TrxId(1), page.NewPageId(1, 1), *pg)

		// WHEN
		size := buf.Size()

		// THEN
		assert.Equal(t, recordHeaderSize+page.PageSize, size)
	})
}

// setupTestBuffer はテスト用の Buffer を作成する
func setupTestBuffer(t *testing.T) *Buffer {
	t.Helper()
	setupRedoTestDir(t)
	buf, err := NewBuffer()
	if err != nil {
		t.Fatalf("Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = buf.logFile.file.Close() })
	return buf
}

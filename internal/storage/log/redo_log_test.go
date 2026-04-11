package log

import (
	"minesql/internal/storage/page"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRedoLog(t *testing.T) {
	t.Run("新規ファイルが作成される", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()

		// WHEN
		rl, err := NewRedoLog(tmpDir)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, rl)
		assert.Equal(t, LSN(0), rl.flushedLSN)
	})

	t.Run("既存ファイルから FlushedLSN が復元される", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl1, err := NewRedoLog(tmpDir)
		assert.NoError(t, err)
		rl1.AppendCommit(1)
		err = rl1.Flush()
		assert.NoError(t, err)
		flushed := rl1.flushedLSN

		// WHEN
		rl2, err := NewRedoLog(tmpDir)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, flushed, rl2.flushedLSN)
	})
}

func TestAppendPageCopy(t *testing.T) {
	t.Run("LSN が単調増加する", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)

		// WHEN
		lsn1 := rl.AppendPageCopy(1, page.NewPageId(1, 0), make([]byte, 4096))
		lsn2 := rl.AppendPageCopy(1, page.NewPageId(1, 1), make([]byte, 4096))

		// THEN
		assert.Equal(t, LSN(1), lsn1)
		assert.Equal(t, LSN(2), lsn2)
	})
}

func TestAppendCommit(t *testing.T) {
	t.Run("COMMIT レコードが追加される", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)

		// WHEN
		lsn := rl.AppendCommit(1)

		// THEN
		assert.Equal(t, LSN(1), lsn)
	})
}

func TestAppendRollback(t *testing.T) {
	t.Run("ROLLBACK レコードが追加される", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)

		// WHEN
		lsn := rl.AppendRollback(1)

		// THEN
		assert.Equal(t, LSN(1), lsn)
	})
}

func TestFlush(t *testing.T) {
	t.Run("バッファの全レコードがディスクに書き込まれる", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)
		rl.AppendCommit(1)
		rl.AppendCommit(2)

		// WHEN
		err := rl.Flush()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, LSN(2), rl.flushedLSN)

		records, err := rl.ReadAll()
		assert.NoError(t, err)
		assert.Equal(t, 2, len(records))
	})

	t.Run("空バッファの場合は何もしない", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)

		// WHEN
		err := rl.Flush()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, LSN(0), rl.flushedLSN)
	})

	t.Run("Flush 後にバッファがクリアされる", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)
		rl.AppendCommit(1)
		err := rl.Flush()
		assert.NoError(t, err)

		// WHEN: 追加の Flush は何も書かない
		err = rl.Flush()

		// THEN
		assert.NoError(t, err)
		records, _ := rl.ReadAll()
		assert.Equal(t, 1, len(records))
	})
}

func TestReadAll(t *testing.T) {
	t.Run("フラッシュ済みのレコードを全て読み取れる", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)

		data := make([]byte, 4096)
		data[0] = 0xAA
		rl.AppendPageCopy(1, page.NewPageId(1, 0), data)
		rl.AppendCommit(1)
		err := rl.Flush()
		assert.NoError(t, err)

		// WHEN
		records, err := rl.ReadAll()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, RedoPageWrite, records[0].Type)
		assert.Equal(t, byte(0xAA), records[0].Data[0])
		assert.Equal(t, RedoCommit, records[1].Type)
	})

	t.Run("空ファイルの場合は nil を返す", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)

		// WHEN
		records, err := rl.ReadAll()

		// THEN
		assert.NoError(t, err)
		assert.Nil(t, records)
	})

	t.Run("複数回の Flush 後に全レコードが読み取れる", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)

		rl.AppendCommit(1)
		err := rl.Flush()
		assert.NoError(t, err)

		rl.AppendCommit(2)
		err = rl.Flush()
		assert.NoError(t, err)

		// WHEN
		records, err := rl.ReadAll()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 2, len(records))
		assert.Equal(t, LSN(1), records[0].LSN)
		assert.Equal(t, LSN(2), records[1].LSN)
	})
}

func TestReset(t *testing.T) {
	t.Run("Reset 後にレコードが空になる", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)
		rl.AppendCommit(1)
		rl.AppendCommit(2)
		err := rl.Flush()
		assert.NoError(t, err)

		// WHEN
		err = rl.Reset()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, LSN(0), rl.flushedLSN)

		records, err := rl.ReadAll()
		assert.NoError(t, err)
		assert.Nil(t, records)
	})

	t.Run("Reset 後に新しいレコードを追加できる", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, _ := NewRedoLog(tmpDir)
		rl.AppendCommit(1)
		err := rl.Flush()
		assert.NoError(t, err)
		err = rl.Reset()
		assert.NoError(t, err)

		// WHEN
		lsn := rl.AppendCommit(10)
		err = rl.Flush()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, LSN(1), lsn)

		records, err := rl.ReadAll()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(records))
		assert.Equal(t, uint64(10), records[0].TrxId)
	})
}

func TestFlushedLSN(t *testing.T) {
	t.Run("新規作成直後は 0 を返す", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, err := NewRedoLog(tmpDir)
		assert.NoError(t, err)

		// WHEN
		lsn := rl.FlushedLSN()

		// THEN
		assert.Equal(t, LSN(0), lsn)
	})

	t.Run("Flush 後にフラッシュ済み LSN が更新される", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, err := NewRedoLog(tmpDir)
		assert.NoError(t, err)
		rl.AppendCommit(1)
		rl.AppendCommit(2)

		// WHEN
		err = rl.Flush()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, LSN(2), rl.FlushedLSN())
	})

	t.Run("Reset 後は 0 に戻る", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, err := NewRedoLog(tmpDir)
		assert.NoError(t, err)
		rl.AppendCommit(1)
		err = rl.Flush()
		assert.NoError(t, err)

		// WHEN
		err = rl.Reset()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, LSN(0), rl.FlushedLSN())
	})
}

func TestFileSize(t *testing.T) {
	t.Run("新規作成直後はヘッダーサイズのみ", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, err := NewRedoLog(tmpDir)
		assert.NoError(t, err)

		// WHEN
		size, err := rl.FileSize()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, int64(16), size) // ヘッダー 16 バイト
	})

	t.Run("Flush 後にファイルサイズが増加する", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, err := NewRedoLog(tmpDir)
		assert.NoError(t, err)
		sizeBefore, _ := rl.FileSize()

		// WHEN
		rl.AppendPageCopy(1, page.NewPageId(1, 0), make([]byte, page.PAGE_SIZE))
		err = rl.Flush()
		assert.NoError(t, err)

		// THEN
		sizeAfter, err := rl.FileSize()
		assert.NoError(t, err)
		assert.Greater(t, sizeAfter, sizeBefore)
	})
}

func TestBufferSize(t *testing.T) {
	t.Run("バッファが空の場合は 0 を返す", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, err := NewRedoLog(tmpDir)
		assert.NoError(t, err)

		// WHEN
		size := rl.BufferSize()

		// THEN
		assert.Equal(t, 0, size)
	})

	t.Run("バッファにレコードがある場合はサイズを返す", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, err := NewRedoLog(tmpDir)
		assert.NoError(t, err)

		// WHEN
		rl.AppendCommit(1)
		size := rl.BufferSize()

		// THEN
		assert.Greater(t, size, 0)
	})

	t.Run("Flush 後はバッファが空になる", func(t *testing.T) {
		// GIVEN
		tmpDir := t.TempDir()
		rl, err := NewRedoLog(tmpDir)
		assert.NoError(t, err)
		rl.AppendCommit(1)

		// WHEN
		err = rl.Flush()
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, 0, rl.BufferSize())
	})
}

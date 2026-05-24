package redo

import (
	"math"
	"sync"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestNewBuffer(t *testing.T) {
	t.Run("Buffer を作成できる", func(t *testing.T) {
		// GIVEN
		dir := t.TempDir()

		// WHEN
		buf, err := NewBuffer(dir)

		// THEN
		assert.NoError(t, err)
		assert.NotNil(t, buf)
		t.Cleanup(func() { _ = buf.Close() })
	})

	t.Run("初期 LSN は 1 から採番される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		lsn, err := buf.AppendCommit(lock.TrxId(1))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(1), lsn)
	})

	t.Run("フラッシュ済みレコードがある場合はその次から採番される", func(t *testing.T) {
		// GIVEN
		dir := t.TempDir()
		buf1, err := NewBuffer(dir)
		assert.NoError(t, err)
		_, _ = buf1.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = buf1.AppendCommit(lock.TrxId(2)) // LSN=2
		_ = buf1.Flush()
		_ = buf1.Close()

		// WHEN
		buf2, err := NewBuffer(dir)
		assert.NoError(t, err)
		t.Cleanup(func() { _ = buf2.Close() })
		lsn, err := buf2.AppendCommit(lock.TrxId(3))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(3), lsn)
	})

	t.Run("flushedLsn が MaxUint32 の場合は ErrLsnOverflow を返す", func(t *testing.T) {
		// GIVEN
		dir := t.TempDir()
		f, err := newFile(dir)
		assert.NoError(t, err)
		f.flushedLsn = math.MaxUint32
		err = f.writeHeader()
		assert.NoError(t, err)
		_ = f.close()

		// WHEN
		_, err = NewBuffer(dir)

		// THEN
		assert.ErrorIs(t, err, ErrLsnOverflow)
	})
}

func TestBufferAppendPageCopy(t *testing.T) {
	t.Run("ページ変更レコードを追加すると LSN を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)

		// WHEN
		lsn, err := buf.AppendPageCopy(lock.TrxId(1), page.NewId(1, 1), pg)

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(1), lsn)
	})

	t.Run("連続して追加するとインクリメントされた LSN を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)

		// WHEN
		lsn1, err := buf.AppendPageCopy(lock.TrxId(1), page.NewId(1, 1), pg)
		assert.NoError(t, err)
		lsn2, err := buf.AppendPageCopy(lock.TrxId(1), page.NewId(1, 2), pg)
		assert.NoError(t, err)

		// THEN
		assert.Equal(t, Lsn(1), lsn1)
		assert.Equal(t, Lsn(2), lsn2)
	})

	t.Run("自動フラッシュ失敗時に追加レコード分の状態がロールバックされる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)
		const recordSize = recordHeaderSize + page.Size
		// 自動フラッシュ閾値を超えない件数までバッファに詰める
		fillCount := maxBufferSize / recordSize
		for range fillCount {
			_, err := buf.AppendPageCopy(lock.TrxId(1), page.NewId(1, 1), pg)
			assert.NoError(t, err)
		}
		// この時点でまだフラッシュは発動していないことを確認
		assert.Equal(t, Lsn(0), buf.FlushedLsn())
		nextLsnBefore := buf.nextLsn
		pendingBefore := buf.pendingSize
		recordsLenBefore := len(buf.records)
		// 強制的に osFile を閉じて以降の flushRecords を失敗させる
		_ = buf.logFile.osFile.Close()

		// WHEN: 次の 1 件で自動フラッシュが発動し失敗する
		lsn, err := buf.AppendPageCopy(lock.TrxId(1), page.NewId(1, 1), pg)

		// THEN
		assert.Error(t, err)
		assert.Equal(t, Lsn(0), lsn)
		assert.Equal(t, nextLsnBefore, buf.nextLsn)
		assert.Equal(t, pendingBefore, buf.pendingSize)
		assert.Equal(t, recordsLenBefore, len(buf.records))
	})
}

func TestBufferAppendCommit(t *testing.T) {
	t.Run("COMMIT レコードを追加できる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		lsn, err := buf.AppendCommit(lock.TrxId(1))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(1), lsn)
	})

	t.Run("COMMIT レコードの data フィールドは nil で保持される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		_, err := buf.AppendCommit(lock.TrxId(1))

		// THEN
		assert.NoError(t, err)
		assert.Len(t, buf.records, 1)
		assert.Nil(t, buf.records[0].data)
	})

	t.Run("複数 goroutine からの追加でも LSN が重複しない", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		const n = 100
		lsns := make([]Lsn, n)
		var wg sync.WaitGroup

		// WHEN
		for i := range n {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				lsn, err := buf.AppendCommit(lock.TrxId(uint32(i + 1)))
				assert.NoError(t, err)
				lsns[i] = lsn
			}(i)
		}
		wg.Wait()

		// THEN
		seen := make(map[Lsn]bool, n)
		for _, lsn := range lsns {
			assert.False(t, seen[lsn], "LSN %d が重複している", lsn)
			seen[lsn] = true
		}
		assert.Len(t, seen, n)
	})
}

func TestBufferAppendRollback(t *testing.T) {
	t.Run("ROLLBACK レコードを追加できる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		lsn, err := buf.AppendRollback(lock.TrxId(1))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(1), lsn)
	})

	t.Run("ROLLBACK レコードの data フィールドは nil で保持される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		_, err := buf.AppendRollback(lock.TrxId(1))

		// THEN
		assert.NoError(t, err)
		assert.Len(t, buf.records, 1)
		assert.Nil(t, buf.records[0].data)
	})
}

func TestBufferReadFrom(t *testing.T) {
	t.Run("LSN 0 を指定すると全レコードを返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)
		_, _ = buf.AppendPageCopy(lock.TrxId(1), page.NewId(1, 1), pg)
		_, _ = buf.AppendCommit(lock.TrxId(1))
		_ = buf.Flush()

		// WHEN
		records, err := buf.ReadFrom(Lsn(0))

		// THEN
		assert.NoError(t, err)
		assert.Len(t, records, 2)
		assert.Equal(t, Lsn(1), records[0].Lsn())
		assert.Equal(t, Lsn(2), records[1].Lsn())
	})

	t.Run("指定 LSN より大きいレコードだけ返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = buf.AppendCommit(lock.TrxId(2)) // LSN=2
		_, _ = buf.AppendCommit(lock.TrxId(3)) // LSN=3
		_ = buf.Flush()

		// WHEN
		records, err := buf.ReadFrom(Lsn(1))

		// THEN
		assert.NoError(t, err)
		assert.Len(t, records, 2)
		assert.Equal(t, Lsn(2), records[0].Lsn())
		assert.Equal(t, Lsn(3), records[1].Lsn())
	})

	t.Run("全レコードが指定 LSN 以下の場合 空を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1)) // LSN=1
		_ = buf.Flush()

		// WHEN
		records, err := buf.ReadFrom(Lsn(5))

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, records)
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

	t.Run("更新した値がファイルに永続化される", func(t *testing.T) {
		// GIVEN
		dir := t.TempDir()
		buf1, err := NewBuffer(dir)
		assert.NoError(t, err)
		_ = buf1.SetCheckpointLsn(Lsn(20))
		_ = buf1.Close()

		// WHEN
		buf2, err := NewBuffer(dir)
		assert.NoError(t, err)
		t.Cleanup(func() { _ = buf2.Close() })

		// THEN
		assert.Equal(t, Lsn(20), buf2.CheckpointLsn())
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
		assert.Equal(t, Lsn(0), lsn)
	})

	t.Run("フラッシュ後は最後のレコードの LSN を返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = buf.AppendCommit(lock.TrxId(2)) // LSN=2
		_ = buf.Flush()

		// WHEN
		lsn := buf.FlushedLsn()

		// THEN
		assert.Equal(t, Lsn(2), lsn)
	})
}

func TestBufferFlush(t *testing.T) {
	t.Run("バッファのレコードをディスクに書き込む", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1))

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
		_, _ = buf.AppendCommit(lock.TrxId(1))

		// WHEN
		_ = buf.Flush()

		// THEN
		assert.Empty(t, buf.records)
		assert.Equal(t, 0, buf.pendingSize)
	})

	t.Run("フラッシュしたレコードが ReadFrom で読み取れる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)
		_, _ = buf.AppendPageCopy(lock.TrxId(1), page.NewId(1, 1), pg)
		_, _ = buf.AppendCommit(lock.TrxId(1))

		// WHEN
		_ = buf.Flush()

		// THEN
		records, err := buf.ReadFrom(Lsn(0))
		assert.NoError(t, err)
		assert.Len(t, records, 2)
		assert.Equal(t, RecordTypePageWrite, records[0].Type())
		assert.Equal(t, RecordTypeCommit, records[1].Type())
	})
}

func TestBufferClear(t *testing.T) {
	t.Run("クリア後にレコードが空になる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1))
		_ = buf.Flush()

		// WHEN
		err := buf.Clear()

		// THEN
		assert.NoError(t, err)
		records, err := buf.ReadFrom(Lsn(0))
		assert.NoError(t, err)
		assert.Empty(t, records)
	})

	t.Run("クリア後に FlushedLsn が 0 になる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1))
		_ = buf.Flush()

		// WHEN
		_ = buf.Clear()

		// THEN
		assert.Equal(t, Lsn(0), buf.FlushedLsn())
	})

	t.Run("未フラッシュレコードがある状態でクリアするとバッファもリセットされる", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1)) // フラッシュせずバッファに残す
		_, _ = buf.AppendCommit(lock.TrxId(2))

		// WHEN
		err := buf.Clear()

		// THEN
		assert.NoError(t, err)
		assert.Empty(t, buf.records)
		assert.Equal(t, 0, buf.pendingSize)
	})

	t.Run("クリア後の LSN は 1 から再採番される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = buf.AppendCommit(lock.TrxId(2)) // LSN=2
		_ = buf.Flush()
		_ = buf.Clear()

		// WHEN
		lsn, err := buf.AppendCommit(lock.TrxId(3))

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, Lsn(1), lsn)
	})
}

func TestBufferClose(t *testing.T) {
	t.Run("Close 後にファイルが閉じられる", func(t *testing.T) {
		// GIVEN
		dir := t.TempDir()
		buf, err := NewBuffer(dir)
		assert.NoError(t, err)

		// WHEN
		err = buf.Close()

		// THEN
		assert.NoError(t, err)
	})
}

func TestBufferTruncateBefore(t *testing.T) {
	t.Run("指定 LSN 以前のレコードが削除される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = buf.AppendCommit(lock.TrxId(2)) // LSN=2
		_, _ = buf.AppendCommit(lock.TrxId(3)) // LSN=3
		_ = buf.Flush()

		// WHEN
		err := buf.TruncateBefore(Lsn(2))

		// THEN
		assert.NoError(t, err)
		records, err := buf.ReadFrom(Lsn(0))
		assert.NoError(t, err)
		assert.Len(t, records, 1)
		assert.Equal(t, Lsn(3), records[0].Lsn())
	})

	t.Run("全レコードの LSN 以上を指定すると全て削除される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1)) // LSN=1
		_, _ = buf.AppendCommit(lock.TrxId(2)) // LSN=2
		_ = buf.Flush()

		// WHEN
		err := buf.TruncateBefore(Lsn(2))

		// THEN
		assert.NoError(t, err)
		records, err := buf.ReadFrom(Lsn(0))
		assert.NoError(t, err)
		assert.Empty(t, records)
	})
}

func TestBufferSize(t *testing.T) {
	t.Run("空のバッファはファイルヘッダーサイズのみ返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)

		// WHEN
		size, err := buf.Size()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, int64(fileHeaderSize), size)
	})

	t.Run("COMMIT レコード追加後はバッファサイズが加算される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1))

		// WHEN
		size, err := buf.Size()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, int64(fileHeaderSize+recordHeaderSize), size)
	})

	t.Run("ページ変更レコード追加後はヘッダー + ページサイズが加算される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		pg := buildTestPage(t)
		_, _ = buf.AppendPageCopy(lock.TrxId(1), page.NewId(1, 1), pg)

		// WHEN
		size, err := buf.Size()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, int64(fileHeaderSize+recordHeaderSize+page.Size), size)
	})

	t.Run("フラッシュ後はファイルサイズのみ返す", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1))
		_ = buf.Flush()

		// WHEN
		size, err := buf.Size()

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, int64(fileHeaderSize+recordHeaderSize), size)
	})

	t.Run("バッファとファイル両方にレコードがある場合は合算される", func(t *testing.T) {
		// GIVEN
		buf := setupTestBuffer(t)
		_, _ = buf.AppendCommit(lock.TrxId(1)) // LSN=1 → バッファ → フラッシュ
		_ = buf.Flush()
		_, _ = buf.AppendCommit(lock.TrxId(2)) // LSN=2 → バッファに残る

		// WHEN
		size, err := buf.Size()

		// THEN
		assert.NoError(t, err)
		// ファイルヘッダー + フラッシュ済みレコード 1 件 + バッファ内レコード 1 件
		expected := int64(fileHeaderSize + recordHeaderSize + recordHeaderSize)
		assert.Equal(t, expected, size)
	})
}

// setupTestBuffer はテスト用の Buffer を作成する
func setupTestBuffer(t *testing.T) *Buffer {
	t.Helper()
	dir := t.TempDir()
	buf, err := NewBuffer(dir)
	if err != nil {
		t.Fatalf("Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = buf.Close() })
	return buf
}

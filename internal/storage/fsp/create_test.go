package fsp

import (
	"path/filepath"
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/file"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/ren-yamanashi/minesql/internal/storage/redo"
	"github.com/stretchr/testify/assert"
)

const testFileId = page.FileId(1)

func TestInitHeader(t *testing.T) {
	t.Run("page 0 が FSP ヘッダーとして初期化される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		if err := InitHeader(mtr, testFileId); err != nil {
			t.Fatalf("InitHeader に失敗: %v", err)
		}
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		bufPage, err := readMtr.PageForRead(page.NewId(testFileId, 0))
		if err != nil {
			t.Fatalf("PageForRead に失敗: %v", err)
		}
		h := header{bufPage: bufPage}
		assert.Equal(t, [4]byte{'M', 'I', 'N', 'E'}, h.magic())
		assert.Equal(t, testFileId, h.fileId())
		assert.Equal(t, uint32(1), h.size())
		assert.Equal(t, page.PageNumber(0), h.freeLimit())
		assert.Equal(t, uint32(0), h.fragNUsed())
		assert.Equal(t, make([]byte, headerSize-headerSegIdOffset), bufPage.Data().Body()[headerSegIdOffset:headerSize])
	})

	t.Run("3 リストが空リストとして初期化される", func(t *testing.T) {
		// GIVEN
		bp, redoLog := setupTest(t)

		// WHEN
		mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
		if err := InitHeader(mtr, testFileId); err != nil {
			t.Fatalf("InitHeader に失敗: %v", err)
		}
		commitMtr(t, mtr)

		// THEN
		readMtr := buffer.NewMtr(bp)
		defer readMtr.UnpinAll()
		h := header{}
		bases := []flst.Address{h.freeListBase(), h.freeFragListBase(), h.fullFragListBase()}
		for _, base := range bases {
			length, err := flst.Length(readMtr, testFileId, base)
			assert.NoError(t, err)
			assert.Equal(t, uint32(0), length)
			first, err := flst.First(readMtr, testFileId, base)
			assert.NoError(t, err)
			assert.True(t, first.IsInvalid())
			last, err := flst.Last(readMtr, testFileId, base)
			assert.NoError(t, err)
			assert.True(t, last.IsInvalid())
		}
	})
}

// setupTest はテスト用の BufferPool と Redo バッファを作成し、指定した PageNumber のゼロ埋めページを用意する
func setupTest(t *testing.T, pageNumbers ...page.PageNumber) (*buffer.Pool, *redo.Buffer) {
	t.Helper()
	redoLog, err := redo.NewBuffer(t.TempDir())
	if err != nil {
		t.Fatalf("redo.Buffer の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = redoLog.Close() })
	path := filepath.Join(t.TempDir(), "test.db")
	hf, err := file.NewHeapFile(testFileId, path)
	if err != nil {
		t.Fatalf("HeapFile の作成に失敗: %v", err)
	}
	t.Cleanup(func() { _ = hf.Close() })
	bp := buffer.NewPool(page.Size*20, redoLog, nil)
	bp.RegisterHeapFile(testFileId, hf)
	for _, pn := range pageNumbers {
		if _, err := bp.AddPage(page.NewId(testFileId, pn)); err != nil {
			t.Fatalf("AddPage に失敗: %v", err)
		}
	}
	return bp, redoLog
}

func commitMtr(t *testing.T, mtr *buffer.Mtr) {
	t.Helper()
	if err := mtr.Commit(); err != nil {
		t.Fatalf("mtr.Commit に失敗: %v", err)
	}
}

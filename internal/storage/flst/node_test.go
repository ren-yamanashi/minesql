package flst

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestReadBaseLength(t *testing.T) {
	t.Run("writeBaseLength で書き込んだ長さを読み取れる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)
		writeBaseLength(bufPage, 40, 7)

		// WHEN
		got := readBaseLength(bufPage, 40)

		// THEN
		assert.Equal(t, uint32(7), got)
		commitMtr(t, mtr)
	})
}

func TestWriteBaseLength(t *testing.T) {
	t.Run("書き込んだ長さが readBaseLength で復元できる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)

		// WHEN
		writeBaseLength(bufPage, 40, 7)

		// THEN
		assert.Equal(t, uint32(7), readBaseLength(bufPage, 40))
		commitMtr(t, mtr)
	})
}

func TestReadBaseFirst(t *testing.T) {
	t.Run("writeBaseFirst で書き込んだアドレスを読み取れる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)
		first := Address{PageNumber: 2, Offset: 100}
		writeBaseFirst(bufPage, 40, first)

		// WHEN
		got := readBaseFirst(bufPage, 40)

		// THEN
		assert.Equal(t, first, got)
		commitMtr(t, mtr)
	})
}

func TestWriteBaseFirst(t *testing.T) {
	t.Run("書き込んだアドレスが readBaseFirst で復元できる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)
		first := Address{PageNumber: 2, Offset: 100}

		// WHEN
		writeBaseFirst(bufPage, 40, first)

		// THEN
		assert.Equal(t, first, readBaseFirst(bufPage, 40))
		commitMtr(t, mtr)
	})
}

func TestReadBaseLast(t *testing.T) {
	t.Run("writeBaseLast で書き込んだアドレスを読み取れる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)
		last := Address{PageNumber: 3, Offset: 200}
		writeBaseLast(bufPage, 40, last)

		// WHEN
		got := readBaseLast(bufPage, 40)

		// THEN
		assert.Equal(t, last, got)
		commitMtr(t, mtr)
	})
}

func TestWriteBaseLast(t *testing.T) {
	t.Run("書き込んだアドレスが readBaseLast で復元できる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)
		last := Address{PageNumber: 3, Offset: 200}

		// WHEN
		writeBaseLast(bufPage, 40, last)

		// THEN
		assert.Equal(t, last, readBaseLast(bufPage, 40))
		commitMtr(t, mtr)
	})
}

func TestReadNodePrev(t *testing.T) {
	t.Run("writeNodePrev で書き込んだアドレスを読み取れる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)
		prev := Address{PageNumber: 5, Offset: 300}
		writeNodePrev(bufPage, 100, prev)

		// WHEN
		got := readNodePrev(bufPage, 100)

		// THEN
		assert.Equal(t, prev, got)
		commitMtr(t, mtr)
	})
}

func TestWriteNodePrev(t *testing.T) {
	t.Run("書き込んだアドレスが readNodePrev で復元できる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)
		prev := Address{PageNumber: 5, Offset: 300}

		// WHEN
		writeNodePrev(bufPage, 100, prev)

		// THEN
		assert.Equal(t, prev, readNodePrev(bufPage, 100))
		commitMtr(t, mtr)
	})
}

func TestReadNodeNext(t *testing.T) {
	t.Run("writeNodeNext で書き込んだアドレスを読み取れる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)
		next := Address{PageNumber: 6, Offset: 400}
		writeNodeNext(bufPage, 100, next)

		// WHEN
		got := readNodeNext(bufPage, 100)

		// THEN
		assert.Equal(t, next, got)
		commitMtr(t, mtr)
	})
}

func TestWriteNodeNext(t *testing.T) {
	t.Run("書き込んだアドレスが readNodeNext で復元できる", func(t *testing.T) {
		// GIVEN
		bufPage, mtr := setupNodeTestPage(t)
		next := Address{PageNumber: 6, Offset: 400}

		// WHEN
		writeNodeNext(bufPage, 100, next)

		// THEN
		assert.Equal(t, next, readNodeNext(bufPage, 100))
		commitMtr(t, mtr)
	})
}

// setupNodeTestPage は書き込み用の PageNumber 0 と Mtr を用意する
func setupNodeTestPage(t *testing.T) (*buffer.Page, *buffer.Mtr) {
	t.Helper()
	bp, redoLog := setupTest(t, 0)
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	bufPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
	if err != nil {
		t.Fatalf("PageForWrite に失敗: %v", err)
	}
	return bufPage, mtr
}

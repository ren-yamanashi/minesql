package fsp

import (
	"testing"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/lock"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
	"github.com/stretchr/testify/assert"
)

func TestHeaderMagic(t *testing.T) {
	t.Run("setMagic で書き込んだシグネチャを読み取れる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)
		h.setMagic()

		// WHEN
		got := h.magic()

		// THEN
		assert.Equal(t, [4]byte{'M', 'I', 'N', 'E'}, got)
		commitMtr(t, mtr)
	})
}

func TestHeaderSetMagic(t *testing.T) {
	t.Run("固定値 MINE を書き込める", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)

		// WHEN
		h.setMagic()

		// THEN
		assert.Equal(t, [4]byte{'M', 'I', 'N', 'E'}, h.magic())
		commitMtr(t, mtr)
	})
}

func TestHeaderFileId(t *testing.T) {
	t.Run("setFileId で書き込んだ FileId を読み取れる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)
		h.setFileId(page.FileId(7))

		// WHEN
		got := h.fileId()

		// THEN
		assert.Equal(t, page.FileId(7), got)
		commitMtr(t, mtr)
	})
}

func TestHeaderSetFileId(t *testing.T) {
	t.Run("書き込んだ FileId が fileId で復元できる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)

		// WHEN
		h.setFileId(page.FileId(7))

		// THEN
		assert.Equal(t, page.FileId(7), h.fileId())
		commitMtr(t, mtr)
	})
}

func TestHeaderSize(t *testing.T) {
	t.Run("setSize で書き込んだ論理ページ数を読み取れる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)
		h.setSize(42)

		// WHEN
		got := h.size()

		// THEN
		assert.Equal(t, uint32(42), got)
		commitMtr(t, mtr)
	})
}

func TestHeaderSetSize(t *testing.T) {
	t.Run("書き込んだ論理ページ数が size で復元できる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)

		// WHEN
		h.setSize(42)

		// THEN
		assert.Equal(t, uint32(42), h.size())
		commitMtr(t, mtr)
	})
}

func TestHeaderFreeLimit(t *testing.T) {
	t.Run("setFreeLimit で書き込んだフリーリミットを読み取れる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)
		h.setFreeLimit(page.PageNumber(512))

		// WHEN
		got := h.freeLimit()

		// THEN
		assert.Equal(t, page.PageNumber(512), got)
		commitMtr(t, mtr)
	})
}

func TestHeaderSetFreeLimit(t *testing.T) {
	t.Run("書き込んだフリーリミットが freeLimit で復元できる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)

		// WHEN
		h.setFreeLimit(page.PageNumber(512))

		// THEN
		assert.Equal(t, page.PageNumber(512), h.freeLimit())
		commitMtr(t, mtr)
	})
}

func TestHeaderFragNUsed(t *testing.T) {
	t.Run("setFragNUsed で書き込んだ使用中ページ数を読み取れる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)
		h.setFragNUsed(3)

		// WHEN
		got := h.fragNUsed()

		// THEN
		assert.Equal(t, uint32(3), got)
		commitMtr(t, mtr)
	})
}

func TestHeaderSetFragNUsed(t *testing.T) {
	t.Run("書き込んだ使用中ページ数が fragNUsed で復元できる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)

		// WHEN
		h.setFragNUsed(3)

		// THEN
		assert.Equal(t, uint32(3), h.fragNUsed())
		commitMtr(t, mtr)
	})
}

func TestHeaderFreeListBase(t *testing.T) {
	t.Run("FREE リストの base node アドレスは page 0 の offset 20 を指す", func(t *testing.T) {
		// GIVEN
		h := header{}

		// WHEN
		got := h.freeListBase()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 0, Offset: 20}, got)
	})
}

func TestHeaderFreeFragListBase(t *testing.T) {
	t.Run("FREE_FRAG リストの base node アドレスは page 0 の offset 36 を指す", func(t *testing.T) {
		// GIVEN
		h := header{}

		// WHEN
		got := h.freeFragListBase()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 0, Offset: 36}, got)
	})
}

func TestHeaderFullFragListBase(t *testing.T) {
	t.Run("FULL_FRAG リストの base node アドレスは page 0 の offset 52 を指す", func(t *testing.T) {
		// GIVEN
		h := header{}

		// WHEN
		got := h.fullFragListBase()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 0, Offset: 52}, got)
	})
}

func TestHeaderSegId(t *testing.T) {
	t.Run("setSegId で書き込んだ segment id を読み取れる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)
		h.setSegId(42)

		// WHEN
		got := h.segId()

		// THEN
		assert.Equal(t, uint64(42), got)
		commitMtr(t, mtr)
	})
}

func TestHeaderSetSegId(t *testing.T) {
	t.Run("書き込んだ segment id が segId で復元できる", func(t *testing.T) {
		// GIVEN
		h, mtr := setupHeader(t)

		// WHEN
		h.setSegId(42)

		// THEN
		assert.Equal(t, uint64(42), h.segId())
		commitMtr(t, mtr)
	})
}

func TestHeaderSegInodesFullBase(t *testing.T) {
	t.Run("SEG_INODES_FULL リストの base node アドレスは page 0 の offset 76 を指す", func(t *testing.T) {
		// GIVEN
		h := header{}

		// WHEN
		got := h.segInodesFullBase()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 0, Offset: 76}, got)
	})
}

func TestHeaderSegInodesFreeBase(t *testing.T) {
	t.Run("SEG_INODES_FREE リストの base node アドレスは page 0 の offset 92 を指す", func(t *testing.T) {
		// GIVEN
		h := header{}

		// WHEN
		got := h.segInodesFreeBase()

		// THEN
		assert.Equal(t, flst.Address{PageNumber: 0, Offset: 92}, got)
	})
}

// setupHeader は書き込み用の page 0 を取得し、header wrapper と Mtr を返す
func setupHeader(t *testing.T) (header, *buffer.Mtr) {
	t.Helper()
	bp, redoLog := setupTest(t, 0)
	mtr := buffer.NewWriteMtr(bp, lock.TrxId(1), redoLog)
	bufPage, err := mtr.PageForWrite(page.NewId(testFileId, 0))
	if err != nil {
		t.Fatalf("PageForWrite に失敗: %v", err)
	}
	return header{bufPage: bufPage}, mtr
}

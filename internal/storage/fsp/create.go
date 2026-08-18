package fsp

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// InitHeader は fileId のファイルの page 0 を FSP ヘッダーとして初期化する
//   - page 0 はディスクから読まず、ゼロ埋めページとしてバッファプールに作成される (新規ファイルに対してのみ呼び出せる)
func InitHeader(mtr *buffer.Mtr, fileId page.FileId) error {
	pageId := page.NewId(fileId, 0)
	if _, err := mtr.Pool().AddPage(pageId); err != nil {
		return err
	}
	bufPage, err := mtr.PageForWrite(pageId)
	if err != nil {
		return err
	}
	h := header{bufPage: bufPage}
	h.setMagic()
	h.setFileId(fileId)
	h.setSize(1)
	h.setFreeLimit(0)
	h.setFragNUsed(0)
	h.setSegId(1)
	bases := []flst.Address{
		h.freeListBase(),
		h.freeFragListBase(),
		h.fullFragListBase(),
		h.segInodesFullBase(),
		h.segInodesFreeBase(),
	}
	for _, base := range bases {
		if err := flst.InitBase(mtr, fileId, base); err != nil {
			return err
		}
	}
	return nil
}

// ValidateHeader は fileId のファイルの page 0 が FSP ヘッダーとして初期化済みで、
// 格納された FileId が引数と一致することを確認する
//   - magic 不一致・FileId 不一致はそれぞれ区別できるエラーとして返す
func ValidateHeader(mtr *buffer.Mtr, fileId page.FileId) error {
	bufPage, err := mtr.PageForRead(page.NewId(fileId, 0))
	if err != nil {
		return err
	}
	h := header{bufPage: bufPage}
	if got := h.magic(); string(got[:]) != magicValue {
		return fmt.Errorf("fsp: magic mismatch on page 0 of FileId %d: got %q", fileId, string(got[:]))
	}
	if got := h.fileId(); got != fileId {
		return fmt.Errorf("fsp: FileId mismatch on page 0: expected %d, got %d", fileId, got)
	}
	return nil
}

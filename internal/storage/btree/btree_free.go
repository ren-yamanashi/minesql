package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
)

// FreeStep は B+Tree 全体の解放を 1 単位進める
//   - 解放が完了した (または既に完了していた) 場合は true を返す
//   - 呼び出し側は true が返るまで、呼び出しごとに新しい mtr で繰り返す
func (t *Tree) FreeStep(mtr *buffer.Mtr) (bool, error) {
	leafDone, err := fsp.FreeSegmentStep(mtr, t.metaPageId.FileId(), t.leafSegmentHeaderAt())
	if err != nil {
		return false, err
	}
	if !leafDone {
		return false, nil
	}
	return fsp.FreeSegmentStep(mtr, t.metaPageId.FileId(), t.branchSegmentHeaderAt())
}

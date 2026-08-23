package btree

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/flst"
	"github.com/ren-yamanashi/minesql/internal/storage/fsp"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// reservedPages は悲観挿入が事前確保した分割用ページの集合
//   - leaf 用に 1 枚、branch 用に高さ分のページを別々に保持する
type reservedPages struct {
	leaf   []page.Id
	branch []page.Id
}

// reservePages は悲観挿入で必要になる最大枚数 (leaf 1 + branch height) を事前確保する
//   - Allocate + AddPage + PageForWrite まで済ませ、SMO 本体の書き込みが失敗しないことを保証する
//   - 途中で失敗した場合は、それまでに確保したページを同一 mini-transaction 内で解放してから error を返す
func (t *Tree) reservePages(mtr *buffer.Mtr, height uint64) (*reservedPages, error) {
	fileId := t.MetaPageId().FileId()
	r := &reservedPages{
		leaf:   make([]page.Id, 0, 1),
		branch: make([]page.Id, 0, height),
	}
	if err := t.reserveOne(mtr, fileId, t.leafSegmentHeaderAt(), &r.leaf); err != nil {
		t.returnReserved(mtr, r)
		return nil, err
	}
	for range height {
		if err := t.reserveOne(mtr, fileId, t.branchSegmentHeaderAt(), &r.branch); err != nil {
			t.returnReserved(mtr, r)
			return nil, err
		}
	}
	return r, nil
}

// reserveOne は headerAt の segment から 1 ページを実確保し dest に追記する
//   - Allocate + AddPage + PageForWrite の全ステップを実行し、確保済みページとして dest に加える
//   - 途中失敗時は、その割り当てを補償解放してから error を返す
func (t *Tree) reserveOne(mtr *buffer.Mtr, fileId page.FileId, headerAt flst.Address, dest *[]page.Id) error {
	pageId, err := fsp.AllocateSegmentPage(mtr, fileId, headerAt)
	if err != nil {
		return err
	}
	if _, err := t.bufferPool.AddPage(pageId); err != nil {
		if compErr := fsp.FreeSegmentPage(mtr, headerAt, pageId); compErr != nil {
			panic(fmt.Sprintf("btree: compensation FreeSegmentPage failed after AddPage failure (pageId=%v): %v", pageId, compErr))
		}
		return err
	}
	if _, err := mtr.PageForWrite(pageId); err != nil {
		if compErr := fsp.FreeSegmentPage(mtr, headerAt, pageId); compErr != nil {
			panic(fmt.Sprintf("btree: compensation FreeSegmentPage failed after PageForWrite failure (pageId=%v): %v", pageId, compErr))
		}
		return err
	}
	*dest = append(*dest, pageId)
	return nil
}

// takeLeaf は事前確保した leaf ページを 1 枚取り出す
//   - 空の場合は panic (確保数が上限のため到達不能)
func (r *reservedPages) takeLeaf() page.Id {
	if len(r.leaf) == 0 {
		panic("btree: reservedPages.takeLeaf on empty leaf pool")
	}
	id := r.leaf[len(r.leaf)-1]
	r.leaf = r.leaf[:len(r.leaf)-1]
	return id
}

// takeBranch は事前確保した branch ページを 1 枚取り出す
//   - 空の場合は panic (確保数が上限のため到達不能)
func (r *reservedPages) takeBranch() page.Id {
	if len(r.branch) == 0 {
		panic("btree: reservedPages.takeBranch on empty branch pool")
	}
	id := r.branch[len(r.branch)-1]
	r.branch = r.branch[:len(r.branch)-1]
	return id
}

// releaseUnused は未使用のまま残った事前確保ページを同一 mtr 内で返却する
//   - 返却失敗は続行不能な二重障害として panic
func (t *Tree) releaseUnused(mtr *buffer.Mtr, r *reservedPages) {
	t.returnReserved(mtr, r)
}

// returnReserved は r が保持する全ページを解放する (成功時 / 失敗時の両方から呼ばれる)
//   - 解放失敗は続行不能な二重障害として panic
func (t *Tree) returnReserved(mtr *buffer.Mtr, r *reservedPages) {
	for _, id := range r.leaf {
		if err := fsp.FreeSegmentPage(mtr, t.leafSegmentHeaderAt(), id); err != nil {
			panic(fmt.Sprintf("btree: return of reserved leaf page failed (pageId=%v): %v", id, err))
		}
	}
	r.leaf = r.leaf[:0]
	for _, id := range r.branch {
		if err := fsp.FreeSegmentPage(mtr, t.branchSegmentHeaderAt(), id); err != nil {
			panic(fmt.Sprintf("btree: return of reserved branch page failed (pageId=%v): %v", id, err))
		}
	}
	r.branch = r.branch[:0]
}

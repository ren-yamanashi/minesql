package flst

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// InitBase は base を空リスト (長さ 0、先頭・末尾は無効アドレス) に初期化する
//   - 必要なページは関数内部で mtr により取得される (以下の各操作も同様)
func InitBase(mtr *buffer.Mtr, fileId page.FileId, base Address) error {
	basePage, err := pageForWrite(mtr, fileId, base)
	if err != nil {
		return err
	}
	writeBaseLength(basePage, int(base.Offset), 0)
	writeBaseFirst(basePage, int(base.Offset), InvalidAddress())
	writeBaseLast(basePage, int(base.Offset), InvalidAddress())
	return nil
}

// AddFirst はリストの先頭に node を追加する
//   - base と node のアドレスが同じ場合は panic する
func AddFirst(mtr *buffer.Mtr, fileId page.FileId, base, node Address) error {
	if base == node {
		panic("flst: base and node must be different addresses")
	}
	basePage, err := pageForWrite(mtr, fileId, base)
	if err != nil {
		return err
	}
	nodePage, err := pageForWrite(mtr, fileId, node)
	if err != nil {
		return err
	}
	oldFirst := readBaseFirst(basePage, int(base.Offset))
	var oldFirstPage *buffer.Page
	if !oldFirst.IsInvalid() {
		if oldFirstPage, err = pageForWrite(mtr, fileId, oldFirst); err != nil {
			return err
		}
	}
	writeNodePrev(nodePage, int(node.Offset), InvalidAddress())
	writeNodeNext(nodePage, int(node.Offset), oldFirst)
	if oldFirst.IsInvalid() {
		writeBaseLast(basePage, int(base.Offset), node)
	} else {
		writeNodePrev(oldFirstPage, int(oldFirst.Offset), node)
	}
	writeBaseFirst(basePage, int(base.Offset), node)
	writeBaseLength(basePage, int(base.Offset), readBaseLength(basePage, int(base.Offset))+1)
	return nil
}

// AddLast はリストの末尾に node を追加する
//   - base と node のアドレスが同じ場合は panic する
func AddLast(mtr *buffer.Mtr, fileId page.FileId, base, node Address) error {
	if base == node {
		panic("flst: base and node must be different addresses")
	}
	basePage, err := pageForWrite(mtr, fileId, base)
	if err != nil {
		return err
	}
	nodePage, err := pageForWrite(mtr, fileId, node)
	if err != nil {
		return err
	}
	oldLast := readBaseLast(basePage, int(base.Offset))
	var oldLastPage *buffer.Page
	if !oldLast.IsInvalid() {
		if oldLastPage, err = pageForWrite(mtr, fileId, oldLast); err != nil {
			return err
		}
	}
	writeNodePrev(nodePage, int(node.Offset), oldLast)
	writeNodeNext(nodePage, int(node.Offset), InvalidAddress())
	if oldLast.IsInvalid() {
		writeBaseFirst(basePage, int(base.Offset), node)
	} else {
		writeNodeNext(oldLastPage, int(oldLast.Offset), node)
	}
	writeBaseLast(basePage, int(base.Offset), node)
	writeBaseLength(basePage, int(base.Offset), readBaseLength(basePage, int(base.Offset))+1)
	return nil
}

// Remove はリストから node を取り除く
//   - 取り除いた node 自身の前後アドレスは書き換えない
//   - 空リストに対して呼ぶと panic する
func Remove(mtr *buffer.Mtr, fileId page.FileId, base, node Address) error {
	basePage, err := pageForWrite(mtr, fileId, base)
	if err != nil {
		return err
	}
	nodePage, err := pageForWrite(mtr, fileId, node)
	if err != nil {
		return err
	}
	prev := readNodePrev(nodePage, int(node.Offset))
	next := readNodeNext(nodePage, int(node.Offset))
	var prevPage *buffer.Page
	if !prev.IsInvalid() {
		if prevPage, err = pageForWrite(mtr, fileId, prev); err != nil {
			return err
		}
	}
	var nextPage *buffer.Page
	if !next.IsInvalid() {
		if nextPage, err = pageForWrite(mtr, fileId, next); err != nil {
			return err
		}
	}
	length := readBaseLength(basePage, int(base.Offset))
	if length == 0 {
		panic("flst: cannot remove from empty list")
	}
	if prev.IsInvalid() {
		writeBaseFirst(basePage, int(base.Offset), next)
	} else {
		writeNodeNext(prevPage, int(prev.Offset), next)
	}
	if next.IsInvalid() {
		writeBaseLast(basePage, int(base.Offset), prev)
	} else {
		writeNodePrev(nextPage, int(next.Offset), prev)
	}
	writeBaseLength(basePage, int(base.Offset), length-1)
	return nil
}

// Length はリストの長さを返す
func Length(mtr *buffer.Mtr, fileId page.FileId, base Address) (uint32, error) {
	basePage, err := pageForRead(mtr, fileId, base)
	if err != nil {
		return 0, err
	}
	return readBaseLength(basePage, int(base.Offset)), nil
}

// First はリストの先頭ノードのアドレスを返す (空リストの場合は無効アドレス)
func First(mtr *buffer.Mtr, fileId page.FileId, base Address) (Address, error) {
	basePage, err := pageForRead(mtr, fileId, base)
	if err != nil {
		return InvalidAddress(), err
	}
	return readBaseFirst(basePage, int(base.Offset)), nil
}

// Last はリストの末尾ノードのアドレスを返す (空リストの場合は無効アドレス)
func Last(mtr *buffer.Mtr, fileId page.FileId, base Address) (Address, error) {
	basePage, err := pageForRead(mtr, fileId, base)
	if err != nil {
		return InvalidAddress(), err
	}
	return readBaseLast(basePage, int(base.Offset)), nil
}

// Next は node の次のノードのアドレスを返す (末尾の場合は無効アドレス)
func Next(mtr *buffer.Mtr, fileId page.FileId, node Address) (Address, error) {
	nodePage, err := pageForRead(mtr, fileId, node)
	if err != nil {
		return InvalidAddress(), err
	}
	return readNodeNext(nodePage, int(node.Offset)), nil
}

// Prev は node の前のノードのアドレスを返す (先頭の場合は無効アドレス)
func Prev(mtr *buffer.Mtr, fileId page.FileId, node Address) (Address, error) {
	nodePage, err := pageForRead(mtr, fileId, node)
	if err != nil {
		return InvalidAddress(), err
	}
	return readNodePrev(nodePage, int(node.Offset)), nil
}

func pageForWrite(mtr *buffer.Mtr, fileId page.FileId, addr Address) (*buffer.Page, error) {
	return mtr.PageForWrite(page.NewId(fileId, addr.PageNumber))
}

func pageForRead(mtr *buffer.Mtr, fileId page.FileId, addr Address) (*buffer.Page, error) {
	return mtr.PageForRead(page.NewId(fileId, addr.PageNumber))
}

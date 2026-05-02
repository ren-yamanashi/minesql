package btree

import "github.com/ren-yamanashi/minesql/internal/storage/page"

// leafPosition はリーフページ内の位置情報を保持する
type leafPosition struct {
	leafPageId    page.PageId
	slotNum       int
	found         bool
	numRecords    int
	parentSlotNum int
}

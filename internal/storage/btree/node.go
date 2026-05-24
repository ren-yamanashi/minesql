package btree

import "github.com/ren-yamanashi/minesql/internal/storage/page"

const nodeHeaderSize = 8

const (
	nodeTypeLeaf   = "LEAF    "
	nodeTypeBranch = "BRANCH  "
)

var (
	_ node = (*leafNode)(nil)
	_ node = (*branchNode)(nil)
)

type node interface {
	insert(slotNum int, record Record) bool
	delete(slotNum int)
	canTransferRecord(isLeftSibling bool) bool
	numRecords() int
	record(slotNum int) Record
	searchSlotNum(key []byte) (int, bool)
	isHalfFull() bool
	maxRecordSize() int
}

// ページデータからノードタイプを取得する
func nodeType(pg *page.Page) string {
	return string(pg.Body()[:nodeHeaderSize])
}

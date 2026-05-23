package btree

import "github.com/ren-yamanashi/minesql/internal/storage/page"

const nodeHeaderSize = 8

var (
	nodeTypeLeaf   = []byte("LEAF    ")
	nodeTypeBranch = []byte("BRANCH  ")
)

type node interface {
	// insert はレコードを挿入する
	insert(slotNum int, record Record) bool
	// delete はレコードを削除する
	delete(slotNum int)
	// canTransferRecord は兄弟ノードにレコードを転送できるか判定する
	canTransferRecord(toRight bool) bool
	// numRecords はレコード数を取得する
	numRecords() int
	// record は指定されたスロット番号のレコードを取得する
	record(slotNum int) Record
	// searchSlotNum は指定された key に対応するスロット番号を検索する
	searchSlotNum(key []byte) (int, bool)
	// isHalfFull はノードが半分以上埋まっているかどうかを判定する
	isHalfFull() bool
	// maxRecordSize は自身のノードに格納できる最大のレコードサイズを返す
	maxRecordSize() int
}

// ページデータからノードタイプを取得する
func getNodeType(pg *page.Page) []byte {
	return pg.Body[0:8]
}

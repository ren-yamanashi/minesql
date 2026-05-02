package node

import "github.com/ren-yamanashi/minesql/internal/storage/page"

const nodeHeaderSize = 8

var (
	NodeTypeLeaf   = []byte("LEAF    ")
	NodeTypeBranch = []byte("BRANCH  ")
)

type Node interface {
	// Insert はレコードを挿入する
	Insert(slotNum int, record Record) bool
	// Delete はレコードを削除する
	Delete(slotNum int)
	// CanTransferRecord は兄弟ノードにレコードを転送できるか判定する
	CanTransferRecord(toRight bool) bool
	// NumRecords はレコード数を取得する
	NumRecords() int
	// Record は指定されたスロット番号のレコードを取得する
	Record(slotNum int) Record
	// SearchSlotNum は指定された key に対応するスロット番号を検索する
	SearchSlotNum(key []byte) (int, bool)
	// IsHalfFull はノードが半分以上埋まっているかどうかを判定する
	IsHalfFull() bool
	// maxRecordSize は自身のノードに格納できる最大のレコードサイズを返す
	maxRecordSize() int
}

// ページデータからノードタイプを取得する
func GetNodeType(pg *page.Page) []byte {
	return pg.Body[0:8]
}

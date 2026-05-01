package node

const nodeHeaderSize = 8

var (
	NodeTypeLeaf   = []byte("LEAF    ")
	NodeTypeBranch = []byte("BRANCH  ")
)

type Node interface {
	// Insert はレコードを挿入する
	Insert(slotNum int, record Record) bool
	// Remove はレコードを削除する
	Remove(slotNum int)
	// CanTransferRecord は兄弟ノードにレコードを転送できるか判定する
	CanTransferRecord(toRight bool) bool
	// NumRecords はレコード数を取得する
	NumRecords() int
	// RecordAt は指定されたスロット番号のレコードを取得する
	RecordAt(slotNum int) Record
	// SearchSlotNum は指定された key に対応するスロット番号を検索する
	SearchSlotNum(key []byte) (int, bool)
	// IsHalfFull はノードが半分以上埋まっているかどうかを判定する
	IsHalfFull() bool
	// maxRecordSize は自身のノードに格納できる最大のレコードサイズを返す
	maxRecordSize() int
}

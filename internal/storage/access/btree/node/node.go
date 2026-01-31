package node

const nodeHeaderSize = 8

var (
	NODE_TYPE_LEAF   = []byte("LEAF    ")
	NODE_TYPE_BRANCH = []byte("BRANCH  ")
)

type NodeHeader struct {
	NodeType [8]byte
}

// Node は B+Tree のノードを表す interface
type Node interface {
	// ノードタイプヘッダーを除いたボディ部分を取得する
	Body() []byte
	// key-value ペア数を取得する
	NumPairs() int
	// 指定されたスロット番号の key-value ペアを取得する
	// slotNum: slotted page のスロット番号
	PairAt(slotNum int) Pair
	// キーから、対応するスロット番号 (slotted page のスロット番号) を検索する (二分探索)
	// 見つかった場合: (スロット番号, true)
	// 見つからなかった場合: (0, false)
	SearchSlotNum(key []byte) (int, bool)
	// key-value ペアを挿入する (pageId は value に相当)
	// slotNum: 挿入先のスロット番号
	// pair: 挿入する key-value ペア
	// 戻り値: 挿入に成功したかどうか
	Insert(slotNum int, pair Pair) bool
}

// ページデータからノードタイプを取得する
// 戻り値: NODE_TYPE_LEAF or NODE_TYPE_BRANCH
func GetNodeType(data []byte) []byte {
	return data[0:8]
}

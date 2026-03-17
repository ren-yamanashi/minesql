package node

const nodeHeaderSize = 8

var (
	NODE_TYPE_LEAF   = []byte("LEAF    ")
	NODE_TYPE_BRANCH = []byte("BRANCH  ")
)

type NodeHeader struct {
	NodeType [nodeHeaderSize]byte // ノードタイプ (LEAF or BRANCH)
}

// Node は B+Tree のノードを表す interface
type Node interface {
	// Insert は key-value ペアを挿入する
	//
	// slotNum: 挿入先のスロット番号 (slotted page のスロット番号)
	//
	// pair: 挿入する key-value ペア
	//
	// 戻り値: 挿入に成功したかどうか
	Insert(slotNum int, pair Pair) bool
	// Delete は key-value ペアを削除する
	//
	// slotNum: 削除するペアのスロット番号 (slotted page のスロット番号)
	Delete(slotNum int)
	// CanTransferPair は兄弟ノードにペアを転送できるかどうかを判定する
	//
	// 転送後も半分以上埋まっている場合は true を返す
	//
	// toRight: true の場合は右の兄弟に転送 (末尾ペアを転送)、false の場合は左の兄弟に転送 (先頭ペアを転送)
	CanTransferPair(toRight bool) bool
	// Body はノードタイプヘッダーを除いたボディ部分を取得する (リーフ|ブランチノードヘッダー + Slotted Page のボディ)
	Body() []byte
	// NumPairs は key-value ペア数を取得する
	NumPairs() int
	// PairAt は指定されたスロット番号の key-value ペアを取得する
	//
	// slotNum: slotted page のスロット番号
	PairAt(slotNum int) Pair
	// SearchSlotNum はキーから、対応するスロット番号 (slotted page のスロット番号) を検索する (二分探索)
	//
	// 見つかった場合: (スロット番号, true)
	//
	// 見つからなかった場合: (0, false)
	SearchSlotNum(key []byte) (int, bool)
	// IsHalfFull はブランチノードが半分以上埋まっているかどうかを判定する
	IsHalfFull() bool
}

// ページデータからノードタイプを取得する
//
// 戻り値: NODE_TYPE_LEAF or NODE_TYPE_BRANCH
func GetNodeType(data []byte) []byte {
	return data[0:8]
}

// 二分探索を行う
//
// node: 探索対象のノード
//
// key: 探索するキー
//
// 戻り値:
//
// - 見つかった場合: (要素のインデックス, true)
//
// - 見つからなかった場合: (挿入すべき位置のインデックス, false)
func binarySearch(node Node, key []byte) (int, bool) {
	left := 0
	right := node.NumPairs()

	for left < right {
		mid := left + (right-left)/2
		pair := node.PairAt(mid) // "1ノード=1ページ" であるため、`mid=slotNum` として該当の key-value ペアを取得できる

		// pair のキーと探索するキーを比較
		// -1: pair.Key < key
		// 0: pair.Key == key
		// 1: pair.Key > key
		result := pair.CompareKey(key)

		switch result {
		// キーが見つかった場合、要素のインデックスを返す
		case 0:
			return mid, true
		// キーが見つからない場合、左右どちらに進むべきかを決定する
		// pair.Key < key の場合、右側に進むため left を mid + 1 に更新する (mid の右半分に対して同様の流れで探索を続ける)
		case -1:
			left = mid + 1
		// pair.Key > key の場合、左側に進むため right を mid に更新する (mid の左半分に対して同様の流れで探索を続ける)
		case 1:
			right = mid
		}
	}

	return left, false
}

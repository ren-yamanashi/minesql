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
	Insert(slotNum int, record Record) bool
	Delete(slotNum int)
	CanTransferRecord(toRight bool) bool
	Body() []byte
	NumRecords() int
	RecordAt(slotNum int) Record
	SearchSlotNum(key []byte) (int, bool)
	IsHalfFull() bool
	maxRecordSize() int
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
	right := node.NumRecords()

	for left < right {
		mid := left + (right-left)/2
		record := node.RecordAt(mid) // "1ノード=1ページ" であるため、`mid=slotNum` として該当のレコードを取得できる

		// レコードのキーと探索するキーを比較
		// -1: record.Key < key
		// 0: record.Key == key
		// 1: record.Key > key
		result := record.CompareKey(key)

		switch result {
		// キーが見つかった場合、要素のインデックスを返す
		case 0:
			return mid, true
		// キーが見つからない場合、左右どちらに進むべきかを決定する
		// record.Key < key の場合、右側に進むため left を mid + 1 に更新する (mid の右半分に対して同様の流れで探索を続ける)
		case -1:
			left = mid + 1
		// record.Key > key の場合、左側に進むため right を mid に更新する (mid の左半分に対して同様の流れで探索を続ける)
		case 1:
			right = mid
		}
	}

	return left, false
}

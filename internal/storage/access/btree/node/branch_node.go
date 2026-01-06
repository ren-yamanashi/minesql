package node

import (
	"encoding/binary"
	slottedpage "minesql/internal/storage/access/btree/slotted_page"
	"minesql/internal/storage/disk"
)

const branchHeaderSize = 8

type Header struct {
	RightChildPageId disk.PageId
}

type BranchNode struct {
	// ページデータ全体 (ヘッダー + Slotted Page のボディ)
	data []byte
	// Slotted Page のボディ部分
	body *slottedpage.SlottedPage
}

func NewBranchNode(data []byte) *BranchNode {
	body := slottedpage.NewSlottedPage(data[headerSize:])
	return &BranchNode{
		data: data,
		body: body,
	}
}

// key-value ペア数を取得する
func (bn *BranchNode) NumPairs() int {
	return bn.body.NumSlots()
}

// 指定されたバッファ ID の key-value ペアを取得する
func (bn *BranchNode) PairAt(bufferId int) Pair {
	data := bn.body.Data(bufferId)
	return PairFromBytes(data)
}

// キーから、対応するバッファ ID を検索する
// 見つかった場合: (バッファ ID, true)
// 見つからなかった場合: (0, false)
func (bn *BranchNode) SearchBufferId(key []byte) (int, bool) {
	return binarySearch(bn.NumPairs(), func(bufferId int) int {
		pair := bn.PairAt(bufferId)
		return pair.CompareKey(key)
	})
}

// 最大ペアサイズを取得する
func (bn *BranchNode) MaxPairSize() int {
	return bn.body.Capacity()/2 - 4 // Slotted Page の容量の半分 - キーサイズを格納する 4 バイト (2 で割るのは、 key と value の両方を格納するため)
}

// key-value ペアを挿入する (pageId は value に相当)
// 戻り値: 挿入に成功したかどうか
func (bn *BranchNode) Insert(bufferId int, pair Pair) bool {
	pairBytes := pair.ToBytes()

	if len(pairBytes) > bn.MaxPairSize() {
		return false
	}

	if !bn.body.Insert(bufferId, len(pairBytes)) {
		return false
	}

	copy(bn.body.Data(bufferId), pairBytes)
	return true
}

func (bn *BranchNode) Initialize(key []byte, leftChildPageId disk.PageId, rightChildPageId disk.PageId) {
	bn.body.Initialize()

	// 左子ページ ID を value とした Pair を作成し、挿入
	leftChildPageIdBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(leftChildPageIdBytes, uint64(leftChildPageId))
	keyPair := NewPair(key, leftChildPageIdBytes)

	if !bn.Insert(0, keyPair) {
		panic("new branch must have space")
	}

	// ヘッダー部分に右子ページ ID を設定
	binary.LittleEndian.PutUint64(bn.data[0:8], uint64(rightChildPageId))
}

// 右端の子ページ ID を設定し、最後のペアのキーを返す (右端のペアは削除される)
// 戻り値: 取り出したキー
func (bn *BranchNode) FillRightChild() []byte {
	lastId := bn.NumPairs() - 1
	pair := bn.PairAt(lastId)
	rightChild := binary.LittleEndian.Uint64(pair.Value)
	key := make([]byte, len(pair.Key))

	// キーをコピー
	copy(key, pair.Key)
	bn.body.Remove(lastId)

	// ヘッダー部分に右子ページ ID を設定
	binary.LittleEndian.PutUint64(bn.data[0:8], rightChild)

	return key
}

// ブランチノードを分割しながらペアを挿入する
// 新しいブランチノードの最小キーを返す
func (bn *BranchNode) SplitInsert(newBranchNode *BranchNode, newPair Pair) []byte {
	newBranchNode.body.Initialize()

	for {
		if newBranchNode.isHalfFull() {
			bufferId, _ := bn.SearchBufferId(newPair.Key)
			if !bn.Insert(bufferId, newPair) {
				panic("old branch must have space")
			}
			break
		}

		// バッファ ID 0 のペアの方が新しいペアのキーよりも小さい場合、ペアを新しいブランチノードに移動する
		if bn.PairAt(0).CompareKey(newPair.Key) < 0 {
			bn.Transfer(newBranchNode)
		} else {
			// 新しいペアを新しいブランチノードに挿入し、残りのペアを新しいブランチノードに移動する
			newBranchNode.Insert(newBranchNode.NumPairs(), newPair)
			for !newBranchNode.isHalfFull() {
				bn.Transfer(newBranchNode)
			}
			break
		}
	}

	return newBranchNode.FillRightChild()
}

// リーフノードが半分以上埋まっているかどうかを判定する
func (bn *BranchNode) isHalfFull() bool {
	return 2*bn.body.FreeSpace() < bn.body.Capacity()
}

// 先頭のペアを別のリーフノードに移動する
func (bn *BranchNode) Transfer(dest *BranchNode) {
	nextIndex := dest.NumPairs()
	data := bn.body.Data(0)

	if !dest.body.Insert(nextIndex, len(data)) {
		panic("no space in dest branch")
	}

	copy(dest.body.Data(nextIndex), data)
	bn.body.Remove(0)
}

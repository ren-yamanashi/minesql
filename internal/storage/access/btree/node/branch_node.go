package node

import (
	"encoding/binary"
	slottedpage "minesql/internal/storage/access/btree/slotted_page"
	"minesql/internal/storage/disk"
)

const branchHeaderSize = 8

type Header struct {
	// 右の子ノードのポインタ (ページ ID)
	RightChildPageId disk.OldPageId
}

type BranchNode struct {
	// ページデータ全体 (ノードタイプヘッダー + ブランチノードヘッダー + Slotted Page のボディ)
	data []byte
	// Slotted Page のボディ部分
	body *slottedpage.SlottedPage
}

func NewBranchNode(data []byte) *BranchNode {
	// data[0:8]: ノードタイプ
	// data[8:16]: 右子ページ ID
	// data[16:]: Slotted Page

	// ノードタイプを設定
	copy(data[0:8], NODE_TYPE_BRANCH)

	body := slottedpage.NewSlottedPage(data[nodeHeaderSize+branchHeaderSize:])
	return &BranchNode{
		data: data,
		body: body,
	}
}

// ノードタイプヘッダーを除いたボディ部分を取得する (ブランチノードヘッダー + Slotted Page のボディ)
func (bn *BranchNode) Body() []byte {
	return bn.data[nodeHeaderSize:]
}

// key-value ペア数を取得する
func (bn *BranchNode) NumPairs() int {
	return bn.body.NumSlots()
}

// 指定されたバッファ ID の key-value ペアを取得する
func (bn *BranchNode) PairAt(bufferId int) Pair {
	data := bn.body.Data(bufferId)
	return pairFromBytes(data)
}

// キーから、対応するバッファ ID を検索する (二分探索)
// 見つかった場合: (バッファ ID, true)
// 見つからなかった場合: (0, false)
func (bn *BranchNode) SearchBufferId(key []byte) (int, bool) {
	return binarySearch(bn.NumPairs(), func(bufferId int) int {
		pair := bn.PairAt(bufferId)
		return pair.CompareKey(key)
	})
}

// key-value ペアを挿入する (pageId は value に相当)
// 戻り値: 挿入に成功したかどうか
func (bn *BranchNode) Insert(bufferId int, pair Pair) bool {
	pairBytes := pair.ToBytes()

	if len(pairBytes) > bn.maxPairSize() {
		return false
	}

	if bn.body.Insert(bufferId, len(pairBytes)) {
		copy(bn.body.Data(bufferId), pairBytes)
		return true
	}

	return false
}

// ブランチノードを初期化する (初期化時には、ペア数は 1 つ)
// key: 最初のペアのキー
// leftChildPageId: 最初のペアの value (左の子ページのページ ID)
// rightChildPageId: ヘッダー部分に設定する右の子ページのページ ID
func (bn *BranchNode) Initialize(key []byte, leftChildPageId disk.OldPageId, rightChildPageId disk.OldPageId) {
	bn.body.Initialize()

	// 左の子ページのポインタ (ページ ID) を value とした Pair を作成し
	leftChildPageIdBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(leftChildPageIdBytes, uint64(leftChildPageId))
	keyPair := NewPair(key, leftChildPageIdBytes)

	if !bn.Insert(0, keyPair) {
		panic("new branch must have space")
	}

	// ヘッダー部分に右の子ページのポインタ (ページ ID) を設定
	binary.LittleEndian.PutUint64(bn.Body()[0:8], uint64(rightChildPageId))
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
			bn.transfer(newBranchNode)
		} else {
			// 新しいペアを新しいブランチノードに挿入し、残りのペアを新しいブランチノードに移動する
			newBranchNode.Insert(newBranchNode.NumPairs(), newPair)
			for !newBranchNode.isHalfFull() {
				bn.transfer(newBranchNode)
			}
			break
		}
	}

	return newBranchNode.fillRightChild()
}

// キーから子ページのインデックスを検索する
func (bn *BranchNode) SearchChildIdx(key []byte) int {
	bufferId, found := bn.SearchBufferId(key)
	if found {
		return bufferId + 1
	}
	return bufferId
}

// 指定されたインデックスの、子ページのページ ID を取得する
func (bn *BranchNode) ChildPageIdAt(childIdx int) disk.OldPageId {
	if childIdx == bn.NumPairs() {
		// 右端の子ページ ID を返す
		return disk.OldPageId(binary.LittleEndian.Uint64(bn.Body()[0:8]))
	}
	pair := bn.PairAt(childIdx)
	return disk.OldPageId(binary.LittleEndian.Uint64(pair.Value))
}

// キーから子ページのページ ID を検索する
func (bn *BranchNode) SearchChildPageId(key []byte) disk.OldPageId {
	childIdx := bn.SearchChildIdx(key)
	return bn.ChildPageIdAt(childIdx)
}

// 最大ペアサイズを取得する
func (bn *BranchNode) maxPairSize() int {
	return bn.body.Capacity()/2 - 4 // Slotted Page の容量の半分 - キーサイズを格納する 4 バイト (2 で割るのは、 key と value の両方を格納するため)
}

// リーフノードが半分以上埋まっているかどうかを判定する
func (bn *BranchNode) isHalfFull() bool {
	return 2*bn.body.FreeSpace() < bn.body.Capacity()
}

// 右端の子ページ ID を設定し、最後のペアのキーを返す (右端のペアは削除される)
// 戻り値: 取り出したキー
func (bn *BranchNode) fillRightChild() []byte {
	lastId := bn.NumPairs() - 1
	pair := bn.PairAt(lastId)
	rightChild := binary.LittleEndian.Uint64(pair.Value)
	key := make([]byte, len(pair.Key))

	// キーをコピー
	copy(key, pair.Key)
	bn.body.Remove(lastId)

	// ヘッダー部分に右子ページ ID を設定
	binary.LittleEndian.PutUint64(bn.Body()[0:8], rightChild)

	return key
}

// 先頭のペアを別のリーフノードに移動する
func (bn *BranchNode) transfer(dest *BranchNode) {
	nextIndex := dest.NumPairs()
	data := bn.body.Data(0)

	if !dest.body.Insert(nextIndex, len(data)) {
		panic("no space in dest branch")
	}

	copy(dest.body.Data(nextIndex), data)
	bn.body.Remove(0)
}

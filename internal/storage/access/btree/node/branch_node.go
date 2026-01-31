package node

import (
	"errors"
	slottedpage "minesql/internal/storage/access/btree/slotted_page"
	"minesql/internal/storage/page"
)

const branchHeaderSize = 8

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

// 指定されたスロット番号の key-value ペアを取得する
// slotNum: slotted page のスロット番号
func (bn *BranchNode) PairAt(slotNum int) Pair {
	data := bn.body.Data(slotNum)
	return pairFromBytes(data)
}

// キーから、対応するスロット番号 (slotted page のスロット番号) を検索する (二分探索)
// 見つかった場合: (スロット番号, true)
// 見つからなかった場合: (0, false)
func (bn *BranchNode) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(bn.NumPairs(), func(slotNum int) int {
		pair := bn.PairAt(slotNum)
		return pair.CompareKey(key)
	})
}

// key-value ペアを挿入する (pageId は value に相当)
// slotNum: 挿入先のスロット番号 (slotted page のスロット番号)
// pair: 挿入する key-value ペア
// 戻り値: 挿入に成功したかどうか
func (bn *BranchNode) Insert(slotNum int, pair Pair) bool {
	pairBytes := pair.ToBytes()

	if len(pairBytes) > bn.maxPairSize() {
		return false
	}

	if bn.body.Insert(slotNum, len(pairBytes)) {
		copy(bn.body.Data(slotNum), pairBytes)
		return true
	}

	return false
}

// ブランチノードを初期化する (初期化時には、ペア数は 1 つ)
// key: 最初のペアのキー
// leftChildPageId: 最初のペアの value (左の子ページのページ ID)
// rightChildPageId: ヘッダー部分に設定する右の子ページのページ ID
func (bn *BranchNode) Initialize(key []byte, leftChildPageId page.PageId, rightChildPageId page.PageId) error {
	bn.body.Initialize()

	// 左の子ページのポインタ (ページ ID) を value とした Pair を作成
	keyPair := NewPair(key, leftChildPageId.ToBytes())

	if !bn.Insert(0, keyPair) {
		return errors.New("new branch must have space")
	}

	// ヘッダー部分に右の子ページのポインタ (ページ ID) を設定
	rightChildPageId.WriteTo(bn.Body(), 0)

	return nil
}

// ブランチノードを分割しながらペアを挿入する
// 新しいブランチノードの最小キーを返す
func (bn *BranchNode) SplitInsert(newBranchNode *BranchNode, newPair Pair) ([]byte, error) {
	newBranchNode.body.Initialize()

	for {
		if newBranchNode.isHalfFull() {
			slotNum, _ := bn.SearchSlotNum(newPair.Key)
			if !bn.Insert(slotNum, newPair) {
				return nil, errors.New("old branch must have space")
			}
			break
		}

		// スロット番号 0 のペアの方が新しいペアのキーよりも小さい場合、ペアを新しいブランチノードに移動する
		if bn.PairAt(0).CompareKey(newPair.Key) < 0 {
			err := bn.transfer(newBranchNode)
			if err != nil {
				return nil, err
			}
		} else {
			// 新しいペアを新しいブランチノードに挿入し、残りのペアを新しいブランチノードに移動する
			newBranchNode.Insert(newBranchNode.NumPairs(), newPair)
			for !newBranchNode.isHalfFull() {
				err := bn.transfer(newBranchNode)
				if err != nil {
					return nil, err
				}
			}
			break
		}
	}

	return newBranchNode.fillRightChild(), nil
}

// キーから子ページのスロット番号を検索する
// キーが見つかった場合、そのキー以上の値は右側の子に進むため slotNum + 1 を返す
// キーが見つからない場合、挿入位置の左側の子に進むため slotNum をそのまま返す
func (bn *BranchNode) SearchChildSlotNum(key []byte) int {
	slotNum, found := bn.SearchSlotNum(key)
	if found {
		return slotNum + 1
	}
	return slotNum
}

// 指定されたスロット番号の、子ページのページ ID を取得する
func (bn *BranchNode) ChildPageIdAt(slotNum int) page.PageId {
	if slotNum == bn.NumPairs() {
		// 右端の子ページ ID を返す
		return page.ReadPageIdFrom(bn.Body(), 0)
	}
	pair := bn.PairAt(slotNum)
	return page.PageIdFromBytes(pair.Value)
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
	rightChild := page.PageIdFromBytes(pair.Value)
	key := make([]byte, len(pair.Key))

	// キーをコピー
	copy(key, pair.Key)
	bn.body.Remove(lastId)

	// ブランチノードのヘッダー部分に右子ページ ID を設定
	rightChild.WriteTo(bn.Body(), 0)

	return key
}

// 先頭のペアを別のリーフノードに移動する
func (bn *BranchNode) transfer(dest *BranchNode) error {
	nextIndex := dest.NumPairs()
	data := bn.body.Data(0)

	if !dest.body.Insert(nextIndex, len(data)) {
		return errors.New("no space in dest branch")
	}

	copy(dest.body.Data(nextIndex), data)
	bn.body.Remove(0)
	return nil
}

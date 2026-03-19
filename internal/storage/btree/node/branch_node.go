package node

import (
	"errors"
	slottedpage "minesql/internal/storage/btree/slotted_page"
	"minesql/internal/storage/page"
)

const branchHeaderSize = 8

type BranchNode struct {
	data []byte                   // ページデータ全体 (ノードタイプヘッダー + ブランチノードヘッダー + Slotted Page のボディ)
	body *slottedpage.SlottedPage // Slotted Page のボディ部分
}

// NewBranchNode はページデータを受け取ってそのデータをブランチノードとして扱うための構造体を返す
//
// data: ページデータ全体
//
// 引数の data はブランチノードとして以下の構成で扱われる
//
// data[0:8]: ノードタイプ
//
// data[8:16]: 右子ページ ID
//
// data[16:]: Slotted Page (16 = nodeHeaderSize + branchHeaderSize)
func NewBranchNode(data []byte) *BranchNode {
	// ノードタイプを設定
	copy(data[0:8], NODE_TYPE_BRANCH)

	// data[16:] 以降を Slotted Page のボディとして扱う
	body := slottedpage.NewSlottedPage(data[nodeHeaderSize+branchHeaderSize:])

	return &BranchNode{
		data: data,
		body: body,
	}
}

// Initialize はブランチノードを初期化する (初期化時には、ペア数は 1 つ)
//
// key: 最初のペアのキー
//
// leftChildPageId: 最初のペアの value (左の子ページのページ ID)
//
// rightChildPageId: ヘッダー部分に設定する右の子ページのページ ID
func (bn *BranchNode) Initialize(key []byte, leftChildPageId page.PageId, rightChildPageId page.PageId) error {
	bn.body.Initialize()

	// 左の子ページのポインタ (ページ ID) を value とした Pair を作成
	pair := NewPair(key, leftChildPageId.ToBytes())

	if !bn.Insert(0, pair) {
		return errors.New("new branch must have space")
	}

	// ヘッダー部分に右の子ページのポインタ (ページ ID) を設定
	rightChildPageId.WriteTo(bn.Body(), 0)

	return nil
}

// Insert は key-value ペアを挿入する
//
// slotNum: 挿入先のスロット番号 (slotted page のスロット番号)
//
// pair: 挿入する key-value ペア
//
// 戻り値: 挿入に成功したかどうか
func (bn *BranchNode) Insert(slotNum int, pair Pair) bool {
	pairBytes := pair.ToBytes()

	if len(pairBytes) > bn.maxPairSize() {
		return false
	}

	return bn.body.Insert(slotNum, pairBytes)
}

// SplitInsert はブランチノードを分割しながらペアを挿入する
//
// newBranchNode: 分割後の新しいブランチノード
//
// newPair: 挿入する key-value ペア
//
// 戻り値: 新しいブランチノードの最小キー
func (bn *BranchNode) SplitInsert(newBranchNode *BranchNode, newPair Pair) ([]byte, error) {
	newBranchNode.body.Initialize()

	for {
		if newBranchNode.IsHalfFull() {
			slotNum, _ := bn.SearchSlotNum(newPair.Key)
			if !bn.Insert(slotNum, newPair) {
				return nil, errors.New("old branch must have space")
			}
			break
		}

		// "古いノードの先頭 (スロット番号=0) のペアのキー < 新しいペアのキー" の場合
		// ペアを新しいブランチノードに移動する
		if bn.PairAt(0).CompareKey(newPair.Key) < 0 {
			err := bn.transfer(newBranchNode)
			if err != nil {
				return nil, err
			}
		} else {
			// 新しいペアを新しいブランチノードに挿入し、残りのペアを新しいブランチノードに移動する
			newBranchNode.Insert(newBranchNode.NumPairs(), newPair)
			for !newBranchNode.IsHalfFull() {
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

// Delete は key-value ペアを削除する
//
// slotNum: 削除するペアのスロット番号 (slotted page のスロット番号)
func (bn *BranchNode) Delete(slotNum int) {
	bn.body.Remove(slotNum)
}

// Update は指定されたスロット番号のキーを更新する
//
// 子ノード側でペアの追加・削除が起きて境界値 (最小キー) が変わった場合に、ブランチノード側のキーも更新する (ページ ID は変わらないので value はそのまま)
//
// 戻り値: 更新に成功したかどうか (空き容量不足の場合は false)
func (bn *BranchNode) Update(slotNum int, newKey []byte) bool {
	pair := bn.PairAt(slotNum)
	newPair := NewPair(newKey, pair.Value)
	return bn.body.Update(slotNum, newPair.ToBytes())
}

// CanTransferPair は兄弟ノードにペアを転送できるかどうかを判定する
//
// 転送後も半分以上埋まっている場合は true を返す
//
// toRight: true の場合は右の兄弟に転送 (末尾ペアを転送)、false の場合は左の兄弟に転送 (先頭ペアを転送)
func (bn *BranchNode) CanTransferPair(toRight bool) bool {
	if bn.NumPairs() <= 1 {
		return false
	}

	// 右の兄弟に転送する場合は末尾ペア、左の兄弟に転送する場合は先頭ペアを転送対象とする
	targetIndex := 0
	if toRight {
		targetIndex = bn.NumPairs() - 1
	}
	targetPairData := bn.body.Data(targetIndex)
	targetPairSize := len(targetPairData)

	// 転送後の空き容量を計算
	freeSpaceAfterTransfer := bn.body.FreeSpace() + targetPairSize + 4 // 4 はポインタサイズ

	// 転送後の空き容量が、ノード全体の容量の半分未満であれば (転送後も半分以上埋まっていると判断できるので) 転送可能と判断する
	return 2*freeSpaceAfterTransfer < bn.body.Capacity()
}

// Body はノードタイプヘッダーを除いたボディ部分を取得する (ブランチノードヘッダー + Slotted Page のボディ)
func (bn *BranchNode) Body() []byte {
	return bn.data[nodeHeaderSize:]
}

// NumPairs は key-value ペア数を取得する
func (bn *BranchNode) NumPairs() int {
	return bn.body.NumSlots()
}

// PairAt は指定されたスロット番号の key-value ペアを取得する
//
// slotNum: slotted page のスロット番号
func (bn *BranchNode) PairAt(slotNum int) Pair {
	data := bn.body.Data(slotNum)
	return pairFromBytes(data)
}

// SearchSlotNum はキーから、対応するスロット番号 (slotted page のスロット番号) を検索する (二分探索)
//
// 見つかった場合: (スロット番号, true)
//
// 見つからなかった場合: (0, false)
func (bn *BranchNode) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(bn, key)
}

// SearchChildSlotNum はキーから子ページのスロット番号を検索する
//
// キーが見つかった場合、そのキー以上の値は右側の子に進むため slotNum + 1 を返す
//
// キーが見つからない場合、挿入位置の左側の子に進むため slotNum をそのまま返す
func (bn *BranchNode) SearchChildSlotNum(key []byte) int {
	slotNum, found := bn.SearchSlotNum(key)
	if found {
		return slotNum + 1
	}
	return slotNum
}

// ChildPageIdAt は指定されたスロット番号の、子ページのページ ID を取得する
func (bn *BranchNode) ChildPageIdAt(slotNum int) page.PageId {
	if slotNum == bn.NumPairs() {
		// 右端の子ページ ID を返す
		return page.ReadPageIdFromPageData(bn.Body(), 0)
	}
	pair := bn.PairAt(slotNum)
	return page.RestorePageIdFromBytes(pair.Value)
}

// RightChildPageId は右端の子ページ ID を取得する
func (bn *BranchNode) RightChildPageId() page.PageId {
	return page.ReadPageIdFromPageData(bn.Body(), 0)
}

// SetRightChildPageId は右端の子ページ ID を設定する
func (bn *BranchNode) SetRightChildPageId(pageId page.PageId) {
	pageId.WriteTo(bn.Body(), 0)
}

// TransferAllFrom は src のすべてのペアを自分の末尾に転送する (src のペアはすべて削除される)
func (bn *BranchNode) TransferAllFrom(src *BranchNode) {
	src.body.TransferAllTo(bn.body)
}

// IsHalfFull はブランチノードが半分以上埋まっているかどうかを判定する
func (bn *BranchNode) IsHalfFull() bool {
	return 2*bn.body.FreeSpace() < bn.body.Capacity()
}

// fillRightChild は右端の子ページ ID を設定し、最後のペアのキーを返す (右端のペアは削除される)
// 戻り値: 取り出したキー
func (bn *BranchNode) fillRightChild() []byte {
	lastId := bn.NumPairs() - 1
	pair := bn.PairAt(lastId)
	rightChild := page.RestorePageIdFromBytes(pair.Value)
	key := make([]byte, len(pair.Key))

	// キーをコピー
	copy(key, pair.Key)
	bn.body.Remove(lastId)

	// ブランチノードのヘッダー部分に右子ページ ID を設定
	rightChild.WriteTo(bn.Body(), 0)

	return key
}

// maxPairSize は最大ペアサイズを取得する
func (bn *BranchNode) maxPairSize() int {
	return bn.body.Capacity()/2 - 4 // Slotted Page の容量の半分 - キーサイズを格納する 4 バイト (2 で割るのは、 key と value の両方を格納するため)
}

// transfer は先頭のペアを別のブランチノードに移動する
func (bn *BranchNode) transfer(dest *BranchNode) error {
	nextIndex := dest.NumPairs()
	data := bn.body.Data(0)

	if !dest.body.Insert(nextIndex, data) {
		return errors.New("no space in dest branch")
	}

	bn.body.Remove(0)
	return nil
}

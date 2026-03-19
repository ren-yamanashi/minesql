package node

import (
	"errors"
	slottedpage "minesql/internal/storage/btree/slotted_page"
	"minesql/internal/storage/page"
)

const leafHeaderSize = 16

type LeafNode struct {
	data []byte                   // ページデータ全体 (ノードタイプヘッダー + リーフノードヘッダー + Slotted Page のボディ)
	body *slottedpage.SlottedPage // Slotted Page のボディ部分
}

// NewLeafNode はページデータを受け取ってそのデータをリーフノードとして扱うための構造体を返す
//
// data: ページデータ全体
//
// 引数の data はリーフノードとして以下の構成で扱われる
//
// data[0:8]: ノードタイプ
//
// data[8:16]: 前ページ ID
//
// data[16:24]: 次ページ ID
//
// data[24:]: Slotted Page (24 = nodeHeaderSize + leafHeaderSize)
func NewLeafNode(data []byte) *LeafNode {
	// ノードタイプを設定
	copy(data[0:8], NODE_TYPE_LEAF)

	// data[24:] 以降を Slotted Page のボディとして扱う
	body := slottedpage.NewSlottedPage(data[nodeHeaderSize+leafHeaderSize:])

	return &LeafNode{
		data: data,
		body: body,
	}
}

// Initialize はリーフノードを初期化する
//
// 初期化時には、前後のリーフノードのポインタ (ページ ID) には無効値が設定される
func (ln *LeafNode) Initialize() {
	page.INVALID_PAGE_ID.WriteTo(ln.Body(), 0) // 初期化時には、前のページ ID を無効値に設定
	page.INVALID_PAGE_ID.WriteTo(ln.Body(), 8) // 初期化時には、次のページ ID を無効値に設定
	ln.body.Initialize()
}

// Insert は key-value ペアを挿入する
//
// slotNum: 挿入先のスロット番号 (slotted page のスロット番号)
//
// pair: 挿入する key-value ペア
//
// 戻り値: 挿入に成功したかどうか
func (ln *LeafNode) Insert(slotNum int, pair Pair) bool {
	pairBytes := pair.ToBytes()

	if len(pairBytes) > ln.maxPairSize() {
		return false
	}

	return ln.body.Insert(slotNum, pairBytes)
}

// SplitInsert はリーフノードを分割しながらペアを挿入する
//
// newLeafNode: 分割後の新しいリーフノード
//
// newPair: 挿入する key-value ペア
//
// 戻り値: 新しいリーフノードの最小キー
func (ln *LeafNode) SplitInsert(newLeafNode *LeafNode, newPair Pair) ([]byte, error) {
	newLeafNode.Initialize()

	for {
		if newLeafNode.IsHalfFull() {
			slotNum, _ := ln.SearchSlotNum(newPair.Key)
			if !ln.Insert(slotNum, newPair) {
				return nil, errors.New("old leaf must have space")
			}
			break
		}

		// "古いノードの先頭 (スロット番号=0) のペアのキー < 新しいペアのキー" の場合
		// ペアを新しいリーフノードに移動する
		if ln.PairAt(0).CompareKey(newPair.Key) < 0 {
			err := ln.transfer(newLeafNode)
			if err != nil {
				return nil, err
			}
		} else {
			// 新しいペアを新しいリーフノードに挿入し、残りのペアを新しいリーフノードに移動する
			newLeafNode.Insert(newLeafNode.NumPairs(), newPair)
			for !newLeafNode.IsHalfFull() {
				err := ln.transfer(newLeafNode)
				if err != nil {
					return nil, err
				}
			}
			break
		}
	}

	return newLeafNode.PairAt(0).Key, nil
}

// Delete は key-value ペアを削除する
//
// slotNum: 削除するペアのスロット番号 (slotted page のスロット番号)
func (ln *LeafNode) Delete(slotNum int) {
	ln.body.Remove(slotNum)
}

// Update は指定されたスロットのペアの value を新しい値に更新する
//
// slotNum: 更新するペアのスロット番号
//
// pair: 新しい key-value ペア (key は変更されない前提)
//
// 戻り値: 更新に成功したかどうか (空き容量不足の場合は false)
func (ln *LeafNode) Update(slotNum int, pair Pair) bool {
	// slotted page 内の slotNum が示す位置のデータを新しいデータに更新する
	return ln.body.Update(slotNum, pair.ToBytes())
}

// CanTransferPair は兄弟ノードにペアを転送できるかどうかを判定する
//
// 転送後も半分以上埋まっている場合は true を返す
//
// toRight: true の場合は右の兄弟に転送 (末尾ペアを転送)、false の場合は左の兄弟に転送 (先頭ペアを転送)
func (ln *LeafNode) CanTransferPair(toRight bool) bool {
	if ln.NumPairs() <= 1 {
		return false
	}

	// 右の兄弟に転送する場合は末尾ペア、左の兄弟に転送する場合は先頭ペアを転送対象とする
	targetIndex := 0
	if toRight {
		targetIndex = ln.NumPairs() - 1
	}
	targetPairData := ln.body.Data(targetIndex)
	targetPairSize := len(targetPairData)

	// 転送後の空き容量を計算
	freeSpaceAfterTransfer := ln.body.FreeSpace() + targetPairSize + 4 // 4 はポインタサイズ

	// 転送後の空き容量が、ノード全体の容量の半分未満であれば (転送後も半分以上埋まっていると判断できるので) 転送可能と判断する
	return 2*freeSpaceAfterTransfer < ln.body.Capacity()
}

// Body はノードタイプヘッダーを除いたボディ部分を取得する (リーフノードヘッダー + Slotted Page のボディ)
func (ln *LeafNode) Body() []byte {
	return ln.data[nodeHeaderSize:]
}

// NumPairs は key-value ペア数を取得する
func (ln *LeafNode) NumPairs() int {
	return ln.body.NumSlots()
}

// PairAt は指定されたスロット番号の key-value ペアを取得する
//
// slotNum: slotted page のスロット番号
func (ln *LeafNode) PairAt(slotNum int) Pair {
	data := ln.body.Data(slotNum)
	return pairFromBytes(data)
}

// SearchSlotNum はキーから、対応するスロット番号 (slotted page のスロット番号) を検索する (二分探索)
//
// 見つかった場合: (スロット番号, true)
//
// 見つからなかった場合: (0, false)
func (ln *LeafNode) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(ln, key)
}

// PrevPageId は前のリーフノードのページ ID を取得する
//
// 前のリーフノードが存在しない場合は nil を返す
func (ln *LeafNode) PrevPageId() *page.PageId {
	pageId := page.ReadPageIdFromPageData(ln.Body(), 0)
	if pageId.IsInvalid() {
		return nil
	}
	return &pageId
}

// NextPageId は次のリーフノードのページ ID を取得する
//
// 次のリーフノードが存在しない場合は nil を返す
func (ln *LeafNode) NextPageId() *page.PageId {
	pageId := page.ReadPageIdFromPageData(ln.Body(), 8)
	if pageId.IsInvalid() {
		return nil
	}
	return &pageId
}

// SetPrevPageId は前のリーフノードのページ ID を設定する
//
// prevPageId: 前のリーフノードのページ ID (前のリーフノードが存在しない場合は nil を指定する)
func (ln *LeafNode) SetPrevPageId(prevPageId *page.PageId) {
	var pageId page.PageId
	if prevPageId == nil {
		pageId = page.INVALID_PAGE_ID
	} else {
		pageId = *prevPageId
	}
	pageId.WriteTo(ln.Body(), 0)
}

// SetNextPageId は次のリーフノードのページ ID を設定する
//
// nextPageId: 次のリーフノードのページ ID (次のリーフノードが存在しない場合は nil を指定する)
func (ln *LeafNode) SetNextPageId(nextPageId *page.PageId) {
	var pageId page.PageId
	if nextPageId == nil {
		pageId = page.INVALID_PAGE_ID
	} else {
		pageId = *nextPageId
	}
	pageId.WriteTo(ln.Body(), 8)
}

// TransferAllFrom は src のすべてのペアを自分の末尾に転送する (src のペアはすべて削除される)
func (ln *LeafNode) TransferAllFrom(src *LeafNode) {
	src.body.TransferAllTo(ln.body)
}

// IsHalfFull はブランチノードが半分以上埋まっているかどうかを判定する
func (ln *LeafNode) IsHalfFull() bool {
	return 2*ln.body.FreeSpace() < ln.body.Capacity()
}

// maxPairSize はリーフノード内の最大ペアサイズを取得する
func (ln *LeafNode) maxPairSize() int {
	return ln.body.Capacity()/2 - 4 // Slotted Page の容量の半分 - キーサイズを格納する 4 バイト (2 で割るのは、 key と value の両方を格納するため)
}

// transfer は先頭のペアを別のリーフノードに移動する
func (ln *LeafNode) transfer(dest *LeafNode) error {
	nextIndex := dest.NumPairs()
	data := ln.body.Data(0)

	if !dest.body.Insert(nextIndex, data) {
		return errors.New("no space in dest leaf")
	}

	ln.body.Remove(0)
	return nil
}

package node

import (
	"errors"
	slottedpage "minesql/internal/storage/access/btree/slotted_page"
	"minesql/internal/storage/page"
)

const leafHeaderSize = 16

type LeafNode struct {
	// ページデータ全体 (ノードタイプヘッダー + リーフノードヘッダー + Slotted Page のボディ)
	data []byte
	// Slotted Page のボディ部分
	body *slottedpage.SlottedPage
}

func NewLeafNode(data []byte) *LeafNode {
	// data[0:8]: ノードタイプ
	// data[8:16]: 前ページ ID
	// data[16:24]: 次ページ ID
	// data[24:]: Slotted Page

	// ノードタイプを設定
	copy(data[0:8], NODE_TYPE_LEAF)

	body := slottedpage.NewSlottedPage(data[nodeHeaderSize+leafHeaderSize:])
	return &LeafNode{
		data: data,
		body: body,
	}
}

// ノードタイプヘッダーを除いたボディ部分を取得する (リーフノードヘッダー + Slotted Page のボディ)
func (ln *LeafNode) Body() []byte {
	return ln.data[nodeHeaderSize:]
}

// key-value ペア数を取得する
func (ln *LeafNode) NumPairs() int {
	return ln.body.NumSlots()
}

// 指定されたスロット番号の key-value ペアを取得する
// slotNum: slotted page のスロット番号
func (ln *LeafNode) PairAt(slotNum int) Pair {
	data := ln.body.Data(slotNum)
	return pairFromBytes(data)
}

// キーから、対応するスロット番号 (slotted page のスロット番号) を検索する (二分探索)
// 見つかった場合: (スロット番号, true)
// 見つからなかった場合: (0, false)
func (ln *LeafNode) SearchSlotNum(key []byte) (int, bool) {
	return binarySearch(ln.NumPairs(), func(slotNum int) int {
		pair := ln.PairAt(slotNum)
		return pair.CompareKey(key)
	})
}

// key-value ペアを挿入する
// slotNum: 挿入先のスロット番号 (slotted page のスロット番号)
// pair: 挿入する key-value ペア
// 戻り値: 挿入に成功したかどうか
func (ln *LeafNode) Insert(slotNum int, pair Pair) bool {
	pairBytes := pair.ToBytes()

	if len(pairBytes) > ln.maxPairSize() {
		return false
	}

	return ln.body.Insert(slotNum, pairBytes)
}

// key-value ペアを削除する
// slotNum: 削除するペアのスロット番号 (slotted page のスロット番号)
func (ln *LeafNode) Delete(slotNum int) {
	ln.body.Remove(slotNum)
}

// 指定されたスロットのペアの value を新しい値に更新する
// slotNum: 更新するペアのスロット番号
// pair: 新しい key-value ペア (key は変更されない前提)
// 戻り値: 更新に成功したかどうか (空き容量不足の場合は false)
func (ln *LeafNode) Update(slotNum int, pair Pair) bool {
	pairBytes := pair.ToBytes()

	// slotted page 内のペアのサイズをリサイズ
	if !ln.body.Resize(slotNum, len(pairBytes)) {
		return false
	}

	// ペアの値を新しい値に更新
	copy(ln.body.Data(slotNum), pairBytes)
	return true
}

// リーフノードを初期化する
// 初期化時には、前後のリーフノードのポインタ (ページ ID) には無効値が設定される
func (ln *LeafNode) Initialize() {
	page.INVALID_PAGE_ID.WriteTo(ln.Body(), 0) // 初期化時には、前のページ ID を無効値に設定
	page.INVALID_PAGE_ID.WriteTo(ln.Body(), 8) // 初期化時には、次のページ ID を無効値に設定
	ln.body.Initialize()
}

// リーフノードを分割しながらペアを挿入する
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

		// スロット番号 0 のペアの方が新しいペアのキーよりも小さい場合、ペアを新しいリーフノードに移動する
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

func (ln *LeafNode) PrevPageId() *page.PageId {
	pageId := page.ReadPageIdFromPageData(ln.Body(), 0)
	if pageId.IsInvalid() {
		return nil
	}
	return &pageId
}

func (ln *LeafNode) NextPageId() *page.PageId {
	pageId := page.ReadPageIdFromPageData(ln.Body(), 8)
	if pageId.IsInvalid() {
		return nil
	}
	return &pageId
}

func (ln *LeafNode) SetPrevPageId(prevPageId *page.PageId) {
	var pageId page.PageId
	if prevPageId == nil {
		pageId = page.INVALID_PAGE_ID
	} else {
		pageId = *prevPageId
	}
	pageId.WriteTo(ln.Body(), 0)
}

func (ln *LeafNode) SetNextPageId(nextPageId *page.PageId) {
	var pageId page.PageId
	if nextPageId == nil {
		pageId = page.INVALID_PAGE_ID
	} else {
		pageId = *nextPageId
	}
	pageId.WriteTo(ln.Body(), 8)
}

// src のすべてのペアを自分の末尾に転送する (src のペアはすべて削除される)
func (ln *LeafNode) TransferAllFrom(src *LeafNode) {
	src.body.TransferAllTo(ln.body)
}

// リーフノードが半分以上埋まっているかどうかを判定する
func (ln *LeafNode) IsHalfFull() bool {
	return 2*ln.body.FreeSpace() < ln.body.Capacity()
}

// 兄弟ノードにペアを貸せるかどうかを判定する
// 貸した後も半分以上埋まっている場合は true を返す
func (ln *LeafNode) CanLendPair() bool {
	if ln.NumPairs() <= 1 {
		return false
	}
	// 先頭ペアを貸した後も半分以上埋まっているかを確認
	firstPairSize := len(ln.body.Data(0))
	freeSpaceAfterLend := ln.body.FreeSpace() + firstPairSize + 4 // 4 はポインタサイズ
	return 2*freeSpaceAfterLend < ln.body.Capacity()
}

// 最大ペアサイズを取得する
func (ln *LeafNode) maxPairSize() int {
	return ln.body.Capacity()/2 - 4 // Slotted Page の容量の半分 - キーサイズを格納する 4 バイト (2 で割るのは、 key と value の両方を格納するため)
}

// 先頭のペアを別のリーフノードに移動する
func (ln *LeafNode) transfer(dest *LeafNode) error {
	nextIndex := dest.NumPairs()
	data := ln.body.Data(0)

	if !dest.body.Insert(nextIndex, data) {
		return errors.New("no space in dest leaf")
	}

	ln.body.Remove(0)
	return nil
}

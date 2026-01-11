package node

import (
	slottedpage "minesql/internal/storage/access/btree/slotted_page"
	"minesql/internal/storage/disk"
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

// 指定されたバッファ ID の key-value ペアを取得する
func (ln *LeafNode) PairAt(bufferId int) Pair {
	data := ln.body.Data(bufferId)
	return pairFromBytes(data)
}

// キーから、対応するバッファ ID を検索する (二分探索)
// 見つかった場合: (バッファ ID, true)
// 見つからなかった場合: (0, false)
func (ln *LeafNode) SearchBufferId(key []byte) (int, bool) {
	return binarySearch(ln.NumPairs(), func(bufferId int) int {
		pair := ln.PairAt(bufferId)
		return pair.CompareKey(key)
	})
}

// key-value ペアを挿入する
// 戻り値: 挿入に成功したかどうか
func (ln *LeafNode) Insert(bufferId int, pair Pair) bool {
	pairBytes := pair.ToBytes()

	if len(pairBytes) > ln.maxPairSize() {
		return false
	}

	if ln.body.Insert(bufferId, len(pairBytes)) {
		copy(ln.body.Data(bufferId), pairBytes)
		return true
	}

	return false
}

// リーフノードを初期化する
// 初期化時には、前後のリーフノードのポインタ (ページ ID) には無効値が設定される
func (ln *LeafNode) Initialize() {
	disk.INVALID_PAGE_ID.WriteTo(ln.Body(), 0)  // 前ページ ID
	disk.INVALID_PAGE_ID.WriteTo(ln.Body(), 8) // 次ページ ID
	ln.body.Initialize()
}

// リーフノードを分割しながらペアを挿入する
// 新しいリーフノードの最小キーを返す
func (ln *LeafNode) SplitInsert(newLeafNode *LeafNode, newPair Pair) []byte {
	newLeafNode.Initialize()

	for {
		if newLeafNode.isHalfFull() {
			bufferId, _ := ln.SearchBufferId(newPair.Key)
			if !ln.Insert(bufferId, newPair) {
				panic("old leaf must have space")
			}
			break
		}

		// バッファ ID 0 のペアの方が新しいペアのキーよりも小さい場合、ペアを新しいリーフノードに移動する
		if ln.PairAt(0).CompareKey(newPair.Key) < 0 {
			ln.transfer(newLeafNode)
		} else {
			// 新しいペアを新しいリーフノードに挿入し、残りのペアを新しいリーフノードに移動する
			newLeafNode.Insert(newLeafNode.NumPairs(), newPair)
			for !newLeafNode.isHalfFull() {
				ln.transfer(newLeafNode)
			}
			break
		}
	}

	return newLeafNode.PairAt(0).Key
}

func (ln *LeafNode) PrevPageId() *disk.PageId {
	pageId := disk.ReadPageIdFrom(ln.Body(), 0)
	if pageId.IsInvalid() {
		return nil
	}
	return &pageId
}

func (ln *LeafNode) NextPageId() *disk.PageId {
	pageId := disk.ReadPageIdFrom(ln.Body(), 8)
	if pageId.IsInvalid() {
		return nil
	}
	return &pageId
}

func (ln *LeafNode) SetPrevPageId(prevPageId *disk.PageId) {
	var pageId disk.PageId
	if prevPageId == nil {
		pageId = disk.INVALID_PAGE_ID
	} else {
		pageId = *prevPageId
	}
	pageId.WriteTo(ln.Body(), 0)
}

func (ln *LeafNode) SetNextPageId(nextPageId *disk.PageId) {
	var pageId disk.PageId
	if nextPageId == nil {
		pageId = disk.INVALID_PAGE_ID
	} else {
		pageId = *nextPageId
	}
	pageId.WriteTo(ln.Body(), 8)
}

// 最大ペアサイズを取得する
func (ln *LeafNode) maxPairSize() int {
	return ln.body.Capacity()/2 - 4 // Slotted Page の容量の半分 - キーサイズを格納する 4 バイト (2 で割るのは、 key と value の両方を格納するため)
}

// リーフノードが半分以上埋まっているかどうかを判定する
func (ln *LeafNode) isHalfFull() bool {
	return 2*ln.body.FreeSpace() < ln.body.Capacity()
}

// 先頭のペアを別のリーフノードに移動する
func (ln *LeafNode) transfer(dest *LeafNode) {
	nextIndex := dest.NumPairs()
	data := ln.body.Data(0)

	if !dest.body.Insert(nextIndex, len(data)) {
		panic("no space in dest branch")
	}

	copy(dest.body.Data(nextIndex), data)
	ln.body.Remove(0)
}

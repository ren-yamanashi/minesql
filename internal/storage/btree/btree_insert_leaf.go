package btree

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// insertLeaf はリーフノードにレコードを挿入する
//   - leafPageId: 挿入先のリーフノードの PageId
//   - leafBufPage: 挿入先のリーフノードのバッファページ
//   - record: 挿入するレコード
//   - reserved: 事前確保済みのページ集合 (分割時に leaf ページを取り出す)
//   - return:
//   - overflowKey: 分割時の境界キー (分割なしの場合は nil)
//   - newPageId: 分割で作られたリーフノードの PageId (分割なしの場合は InvalidPageId)
func (t *Tree) insertLeaf(
	mtr *buffer.Mtr,
	leafPageId page.Id,
	leafBufPage *buffer.Page,
	record Record,
	reserved *reservedPages,
) (overflowKey []byte, newPageId page.Id, err error) {
	leafNode := newLeafNode(leafBufPage)
	slotNum, found := leafNode.searchSlotNum(record.Key())
	if found {
		return nil, page.InvalidId(), ErrDuplicateKey
	}

	// リーフノードに挿入できた場合は終了
	if leafNode.insert(slotNum, record) {
		return nil, page.InvalidId(), nil
	}

	// リーフノードが満杯の場合は分割
	return t.splitInsertLeaf(mtr, leafPageId, leafNode, record, reserved)
}

// splitInsertLeaf はリーフノードを分割してレコードを挿入する
//   - leafPageId: 分割元のリーフノードの PageId
//   - leaf: 分割元のリーフノード
//   - record: 挿入するレコード
//   - reserved: 事前確保済みのページ集合 (leaf ページを 1 枚取り出す)
//   - return: 境界キー, 新しいリーフノードの PageId
func (t *Tree) splitInsertLeaf(
	mtr *buffer.Mtr,
	leafPageId page.Id,
	leaf *leafNode,
	record Record,
	reserved *reservedPages,
) ([]byte, page.Id, error) {
	prevLeafPageId := leaf.prevPageId()

	// 前のリーフノードは分割書き込みより前に触れておく (書き込み前 touch)
	var prevLeaf *leafNode
	if !prevLeafPageId.IsInvalid() {
		pagePrevLeaf, err := mtr.PageForWrite(prevLeafPageId)
		if err != nil {
			return nil, page.InvalidId(), err
		}
		prevLeaf = newLeafNode(pagePrevLeaf)
	}

	// 事前確保済みの leaf ページを取り出す
	newLeafPageId := reserved.takeLeaf()
	pageNewLeaf, err := mtr.PageForWrite(newLeafPageId)
	if err != nil {
		panic(fmt.Sprintf("btree: PageForWrite failed for reserved leaf page (pageId=%v): %v", newLeafPageId, err))
	}
	newLeaf := newLeafNode(pageNewLeaf)

	// 新しいリーフノードに分割挿入 (実体の書き込み)
	overflowKey := leaf.splitInsert(newLeaf, record)

	// 実体を書き終わったあとにリンクを更新する
	newLeaf.setNextPageId(leafPageId)
	newLeaf.setPrevPageId(prevLeafPageId)
	leaf.setPrevPageId(newLeafPageId)
	if prevLeaf != nil {
		prevLeaf.setNextPageId(newLeafPageId)
	}

	return overflowKey, newLeafPageId, nil
}

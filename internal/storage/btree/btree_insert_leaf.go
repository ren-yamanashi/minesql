package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// insertLeaf はリーフノードにレコードを挿入する
//   - leafPageId: 挿入先のリーフノードの PageId
//   - leafPage: 挿入先のリーフノードのページデータ
//   - record: 挿入するレコード
//   - return:
//   - overflowKey: 分割時の境界キー (分割なしの場合は nil)
//   - newPageId: 分割で作られたリーフノードの PageId (分割なしの場合は InvalidPageId)
func (bt *Btree) insertLeaf(leafPageId page.PageId, leafPage *page.Page, record node.Record) (overflowKey []byte, newPageId page.PageId, err error) {
	leafNode := node.NewLeafNode(leafPage)
	slotNum, found := leafNode.SearchSlotNum(record.Key())
	if found {
		return nil, page.InvalidPageId, ErrDuplicateKey
	}

	// リーフノードに挿入できた場合は終了
	if leafNode.Insert(slotNum, record) {
		return nil, page.InvalidPageId, nil
	}

	// リーフノードが満杯の場合は分割
	return bt.splitInsertLeaf(leafPageId, leafNode, record)
}

// splitInsertLeaf はリーフノードを分割してレコードを挿入する
//   - leafPageId: 分割元のリーフノードの PageId
//   - leafNode: 分割元のリーフノード
//   - record: 挿入するレコード
//   - return: 境界キー, 新しいリーフノードの PageId
func (bt *Btree) splitInsertLeaf(
	leafPageId page.PageId,
	leafNode *node.LeafNode,
	record node.Record,
) ([]byte, page.PageId, error) {
	prevLeafPageId := leafNode.PrevPageId()
	if !prevLeafPageId.IsInvalid() {
		defer bt.bufferPool.UnRefPage(prevLeafPageId)
	}

	// 新しいリーフノードを作成
	newLeafPageId, err := bt.bufferPool.AllocatePageId(bt.metaPageId.FileId)
	if err != nil {
		return nil, page.InvalidPageId, err
	}
	_, err = bt.bufferPool.AddPage(newLeafPageId)
	if err != nil {
		return nil, page.InvalidPageId, err
	}
	defer bt.bufferPool.UnRefPage(newLeafPageId)

	// 前のリーフノードが存在する場合は、nextPageId を新しいリーフノードの PageId に更新
	if !prevLeafPageId.IsInvalid() {
		if err := bt.updatePrevLeafLink(prevLeafPageId, newLeafPageId); err != nil {
			return nil, page.InvalidPageId, err
		}
	}

	// 新しいリーフノードに分割挿入
	pageNewLeaf, err := bt.bufferPool.GetWritePage(newLeafPageId)
	if err != nil {
		return nil, page.InvalidPageId, err
	}
	newLeaf := node.NewLeafNode(pageNewLeaf)
	overflowKey, err := leafNode.SplitInsert(newLeaf, record)
	if err != nil {
		return nil, page.InvalidPageId, err
	}

	// ポインタを更新
	newLeaf.SetNextPageId(leafPageId)
	newLeaf.SetPrevPageId(prevLeafPageId)
	leafNode.SetPrevPageId(newLeafPageId)

	return overflowKey, newLeafPageId, nil
}

// updatePrevLeafLink は前のリーフノードの nextPageId を更新する
func (bt *Btree) updatePrevLeafLink(prevLeafPageId, newNextPageId page.PageId) error {
	pagePrevLeaf, err := bt.bufferPool.GetWritePage(prevLeafPageId)
	if err != nil {
		return err
	}
	prevLeaf := node.NewLeafNode(pagePrevLeaf)
	prevLeaf.SetNextPageId(newNextPageId)
	return nil
}

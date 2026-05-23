package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// insertLeaf はリーフノードにレコードを挿入する
//   - leafPageId: 挿入先のリーフノードの PageId
//   - leafPage: 挿入先のリーフノードのページデータ
//   - record: 挿入するレコード
//   - return:
//   - overflowKey: 分割時の境界キー (分割なしの場合は nil)
//   - newPageId: 分割で作られたリーフノードの PageId (分割なしの場合は InvalidPageId)
func (bt *Btree) insertLeaf(leafPageId page.Id, leafPage *page.Page, record Record) (overflowKey []byte, newPageId page.Id, err error) {
	leafNode := NewLeafNode(leafPage)
	slotNum, found := leafNode.SearchSlotNum(record.Key())
	if found {
		return nil, page.InvalidId, ErrDuplicateKey
	}

	// リーフノードに挿入できた場合は終了
	if leafNode.Insert(slotNum, record) {
		return nil, page.InvalidId, nil
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
	leafPageId page.Id,
	leafNode *LeafNode,
	record Record,
) ([]byte, page.Id, error) {
	prevLeafPageId := leafNode.PrevPageId()
	if !prevLeafPageId.IsInvalid() {
		defer bt.bufferPool.UnRefPage(prevLeafPageId)
	}

	// 新しいリーフノードを作成
	newLeafPageId, err := bt.bufferPool.AllocatePageId(bt.MetaPageId.FileId)
	if err != nil {
		return nil, page.InvalidId, err
	}
	_, err = bt.bufferPool.AddPage(newLeafPageId)
	if err != nil {
		return nil, page.InvalidId, err
	}
	defer bt.bufferPool.UnRefPage(newLeafPageId)

	// 前のリーフノードが存在する場合は、nextPageId を新しいリーフノードの PageId に更新
	if !prevLeafPageId.IsInvalid() {
		if err := bt.updatePrevLeafLink(prevLeafPageId, newLeafPageId); err != nil {
			return nil, page.InvalidId, err
		}
	}

	// 新しいリーフノードに分割挿入
	pageNewLeaf, err := bt.bufferPool.BufferPageForWrite(newLeafPageId)
	if err != nil {
		return nil, page.InvalidId, err
	}
	newLeaf := NewLeafNode(pageNewLeaf.Page)
	overflowKey, err := leafNode.SplitInsert(newLeaf, record)
	if err != nil {
		return nil, page.InvalidId, err
	}

	// ポインタを更新
	newLeaf.SetNextPageId(leafPageId)
	newLeaf.SetPrevPageId(prevLeafPageId)
	leafNode.SetPrevPageId(newLeafPageId)

	return overflowKey, newLeafPageId, nil
}

// updatePrevLeafLink は前のリーフノードの nextPageId を更新する
func (bt *Btree) updatePrevLeafLink(prevLeafPageId, newNextPageId page.Id) error {
	pagePrevLeaf, err := bt.bufferPool.BufferPageForWrite(prevLeafPageId)
	if err != nil {
		return err
	}
	prevLeaf := NewLeafNode(pagePrevLeaf.Page)
	prevLeaf.SetNextPageId(newNextPageId)
	return nil
}

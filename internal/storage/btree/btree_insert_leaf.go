package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// insertLeaf はリーフノードにレコードを挿入する
//   - leafPageId: 挿入先のリーフノードの PageId
//   - leafPage: 挿入先のリーフノードのページデータ
//   - record: 挿入するレコード
//   - return:
//   - overflowKey: 分割時の境界キー (分割なしの場合は nil)
//   - newPageId: 分割で作られたリーフノードの PageId (分割なしの場合は InvalidPageId)
func (t *Tree) insertLeaf(
	mtr *buffer.Mtr,
	leafPageId page.Id,
	leafPage *page.Page,
	record Record,
) (overflowKey []byte, newPageId page.Id, err error) {
	leafNode := newLeafNode(leafPage)
	slotNum, found := leafNode.searchSlotNum(record.Key())
	if found {
		return nil, page.InvalidId(), ErrDuplicateKey
	}

	// リーフノードに挿入できた場合は終了
	if leafNode.insert(slotNum, record) {
		return nil, page.InvalidId(), nil
	}

	// リーフノードが満杯の場合は分割
	return t.splitInsertLeaf(mtr, leafPageId, leafNode, record)
}

// splitInsertLeaf はリーフノードを分割してレコードを挿入する
//   - leafPageId: 分割元のリーフノードの PageId
//   - leafNode: 分割元のリーフノード
//   - record: 挿入するレコード
//   - return: 境界キー, 新しいリーフノードの PageId
func (t *Tree) splitInsertLeaf(
	mtr *buffer.Mtr,
	leafPageId page.Id,
	leafNode *leafNode,
	record Record,
) ([]byte, page.Id, error) {
	prevLeafPageId := leafNode.prevPageId()

	// 新しいリーフノードを作成
	newLeafPageId, err := t.bufferPool.AllocatePageId(t.MetaPageId().FileId())
	if err != nil {
		return nil, page.InvalidId(), err
	}
	_, err = t.bufferPool.AddPage(newLeafPageId)
	if err != nil {
		return nil, page.InvalidId(), err
	}
	defer mtr.Unpin(newLeafPageId)

	// 前のリーフノードが存在する場合は、nextPageId を新しいリーフノードの PageId に更新
	if !prevLeafPageId.IsInvalid() {
		if err := t.updatePrevLeafLink(mtr, prevLeafPageId, newLeafPageId); err != nil {
			return nil, page.InvalidId(), err
		}
		defer mtr.Unpin(prevLeafPageId)
	}

	// 新しいリーフノードに分割挿入
	pageNewLeaf, err := mtr.PageForWrite(newLeafPageId)
	if err != nil {
		return nil, page.InvalidId(), err
	}
	newLeaf := newLeafNode(pageNewLeaf.Data())
	overflowKey, err := leafNode.splitInsert(newLeaf, record)
	if err != nil {
		return nil, page.InvalidId(), err
	}

	// ポインタを更新
	newLeaf.setNextPageId(leafPageId)
	newLeaf.setPrevPageId(prevLeafPageId)
	leafNode.setPrevPageId(newLeafPageId)

	return overflowKey, newLeafPageId, nil
}

// updatePrevLeafLink は前のリーフノードの nextPageId を更新する
func (t *Tree) updatePrevLeafLink(mtr *buffer.Mtr, prevLeafPageId, newNextPageId page.Id) error {
	pagePrevLeaf, err := mtr.PageForWrite(prevLeafPageId)
	if err != nil {
		return err
	}
	prevLeaf := newLeafNode(pagePrevLeaf.Data())
	prevLeaf.setNextPageId(newNextPageId)
	return nil
}

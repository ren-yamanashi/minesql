package btree

import (
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Insert は B+Tree にレコードを挿入する
func (t *Tree) Insert(record Record) error {
	// メタページを取得
	pageMeta, err := t.bufferPool.PageForWrite(t.MetaPageId())
	if err != nil {
		return err
	}
	defer t.bufferPool.UnrefPage(t.MetaPageId())
	metaPage := newMetaPage(pageMeta.Data())

	// ルートページを取得
	rootPageId := metaPage.rootPageId()
	rootPageBuf, err := t.bufferPool.PageForRead(rootPageId)
	if err != nil {
		return err
	}
	defer t.bufferPool.UnrefPage(rootPageId)

	// 再帰的に挿入
	overflowKey, overflowChildPageId, isLeafSplit, err := t.insertRecursively(rootPageBuf, record)
	if err != nil {
		return err
	}
	isRootSplit := !overflowChildPageId.IsInvalid()

	// リーフノードの分割もルートノードの分割も発生しなかった場合
	if !isLeafSplit && !isRootSplit {
		return nil
	}

	// リーフノードの分割が発生した場合
	if isLeafSplit {
		metaPage.setLeafPageCount(metaPage.leafPageCount() + 1)
	}
	if !isRootSplit {
		return nil
	}

	// ルートノードの分割が発生した場合
	newRootPageId, err := t.bufferPool.AllocatePageId(t.MetaPageId().FileId())
	if err != nil {
		return err
	}
	_, err = t.bufferPool.AddPage(newRootPageId)
	if err != nil {
		return err
	}
	pageNewRoot, err := t.bufferPool.PageForWrite(newRootPageId)
	if err != nil {
		return err
	}
	newRootBranch := newBranchNode(pageNewRoot.Data())
	err = newRootBranch.initialize(overflowKey, overflowChildPageId, rootPageId)
	if err != nil {
		return err
	}
	metaPage.setRootPageId(newRootPageId)
	metaPage.setHeight(metaPage.height() + 1)
	return nil
}

// insertRecursively は再帰的にノードを辿ってレコードを挿入する
//   - bufPage: 挿入先のノードのバッファページ
//   - record: 挿入するレコード
//   - return:
//   - overflowKey: 分割時の境界キー (分割なしの場合は nil)
//   - newPageId: 分割で作られたノードの PageId (分割なしの場合は InvalidPageId)
//   - isLeafSplit: リーフノードの分割が発生したか
func (t *Tree) insertRecursively(
	bufPage *buffer.Page,
	record Record,
) (overflowKey []byte, newPageId page.Id, isLeafSplit bool, err error) {
	pg, err := t.bufferPool.PageForWrite(bufPage.PageId())
	if err != nil {
		return nil, page.InvalidId(), false, err
	}
	defer t.bufferPool.UnrefPage(bufPage.PageId())
	nt := nodeType(pg.Data())

	switch nt {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case nodeTypeBranch:
		// 挿入先の子ノードを取得
		branchNode := newBranchNode(pg.Data())
		childSlotNum, found := branchNode.searchSlotNum(record.Key())
		if found {
			childSlotNum++ // 境界キーと一致する場合、右の子に属する
		}
		childPageId, err := branchNode.childPageId(childSlotNum)
		if err != nil {
			return nil, page.InvalidId(), false, err
		}
		childBufPage, err := t.bufferPool.PageForRead(childPageId)
		if err != nil {
			return nil, page.InvalidId(), false, err
		}
		defer t.bufferPool.UnrefPage(childPageId)
		// 子ノードに対して挿入処理を再帰的に実行
		overflowKeyFromChild, overflowChildPageId, isLeafSplit, err := t.insertRecursively(childBufPage, record)
		if err != nil {
			return nil, page.InvalidId(), false, err
		}
		// 子ノードが分割されなかった場合、終了
		if overflowChildPageId.IsInvalid() {
			return nil, page.InvalidId(), isLeafSplit, nil
		}
		// 子ノードが分割された場合、ブランチノードにオーバーフローレコードを挿入
		overflowKey, newPageId, err := t.insertBranchOverflow(
			branchNode,
			childSlotNum,
			overflowKeyFromChild,
			overflowChildPageId,
		)
		if err != nil {
			return nil, page.InvalidId(), isLeafSplit, err
		}
		return overflowKey, newPageId, isLeafSplit, nil

	// リーフノードの場合: そのまま挿入する
	case nodeTypeLeaf:
		overflowKey, newPageId, err := t.insertLeaf(bufPage.PageId(), pg.Data(), record)
		if err != nil {
			return nil, page.InvalidId(), false, err
		}
		isSplit := !newPageId.IsInvalid()
		return overflowKey, newPageId, isSplit, nil

	default:
		return nil, page.InvalidId(), false, errUnknownNodeType
	}
}

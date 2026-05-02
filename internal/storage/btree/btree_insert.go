package btree

import (
	"bytes"
	"errors"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/list"
	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Insert は B+Tree にレコードを挿入する
func (bt *Btree) Insert(record node.Record) error {
	// メタページを取得
	pageMeta, err := bt.bufferPool.GetWritePage(bt.MetaPageId)
	if err != nil {
		return err
	}
	defer bt.bufferPool.UnRefPage(bt.MetaPageId)
	meta := newMetaPage(pageMeta)

	// ルートページを取得
	rootPageId := meta.rootPageId()
	rootPageBuf, err := bt.bufferPool.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	// 再帰的に挿入
	overflowKey, overflowChildPageId, isLeafSplit, err := bt.insertRecursively(rootPageBuf, record)
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
		meta.setLeafPageCount(meta.leafPageCount() + 1)
	}
	if !isRootSplit {
		return nil
	}

	// ルートノードの分割が発生した場合
	newRootPageId, err := bt.bufferPool.AllocatePageId(bt.MetaPageId.FileId)
	if err != nil {
		return err
	}
	_, err = bt.bufferPool.AddPage(newRootPageId)
	if err != nil {
		return err
	}
	pageNewRoot, err := bt.bufferPool.GetWritePage(newRootPageId)
	if err != nil {
		return err
	}
	newRootBranch := node.NewBranchNode(pageNewRoot)
	err = newRootBranch.Initialize(overflowKey, overflowChildPageId, rootPageId)
	if err != nil {
		return err
	}
	meta.setRootPageId(newRootPageId)
	meta.setHeight(meta.height() + 1)
	return nil
}

// insertRecursively は再帰的にノードを辿ってレコードを挿入する
//   - bufPage: 挿入先のノードのバッファページ
//   - record: 挿入するレコード
//   - return:
//   - overflowKey: 分割時の境界キー (分割なしの場合は nil)
//   - newPageId: 分割で作られたノードの PageId (分割なしの場合は InvalidPageId)
//   - isLeafSplit: リーフノードの分割が発生したか
func (bt *Btree) insertRecursively(
	bufPage *buffer.BufferPage,
	record node.Record,
) (overflowKey []byte, newPageId page.PageId, isLeafSplit bool, err error) {
	pg, err := bt.bufferPool.GetWritePage(bufPage.PageId)
	if err != nil {
		return nil, page.InvalidPageId, false, err
	}
	nodeType := node.GetNodeType(pg)

	switch {
	// ブランチノードの場合、子ノードに対して再帰実行する
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		// 挿入先の子ノードを取得
		branchNode := node.NewBranchNode(pg)
		childSlotNum, _ := branchNode.SearchSlotNum(record.Key())
		childPageId, err := branchNode.ChildPageId(childSlotNum)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		childBufPage, err := bt.bufferPool.FetchPage(childPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		defer bt.bufferPool.UnRefPage(childPageId)

		// 子ノードに対して挿入処理を再帰的に実行
		overflowKeyFromChild, overflowChildPageId, isLeafSplit, err := bt.insertRecursively(childBufPage, record)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		// 子ノードが分割されなかった場合、終了
		if overflowChildPageId.IsInvalid() {
			return nil, page.InvalidPageId, isLeafSplit, nil
		}
		// 子ノードが分割された場合、ブランチノードにオーバーフローレコードを挿入
		bl := list.NewBranchList(bt.bufferPool, bt.MetaPageId.FileId)
		overflowKey, newPageId, err := bl.InsertOverflow(
			branchNode,
			childSlotNum,
			overflowKeyFromChild,
			overflowChildPageId,
		)
		if err != nil {
			return nil, page.InvalidPageId, isLeafSplit, err
		}
		return overflowKey, newPageId, isLeafSplit, nil

	// リーフノードの場合、そのまま挿入する
	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		ll := list.NewLeafList(bt.bufferPool, bt.MetaPageId.FileId)
		overflowKey, newPageId, err := ll.Insert(bufPage.PageId, pg, record)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		isSplit := !newPageId.IsInvalid()
		return overflowKey, newPageId, isSplit, nil

	default:
		return nil, page.InvalidPageId, false, errors.New("unknown node type")
	}
}

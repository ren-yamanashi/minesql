package btree

import (
	"bytes"

	"github.com/ren-yamanashi/minesql/internal/storage/btree/node"
	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Insert は B+Tree にレコードを挿入する
func (bt *BTree) Insert(bp *buffer.BufferPool, record node.Record) error {
	// メタページを取得
	// メタページは使い終わったらすぐ不要になる (優先的に evict されたい) ので、UnRefPage する
	defer bp.UnRefPage(bt.MetaPageId)
	metaData, err := bp.GetReadPageData(bt.MetaPageId)
	if err != nil {
		return err
	}
	meta := newMetaPage(page.NewPage(metaData))

	// ルートページを取得
	rootPageId := meta.rootPageId()
	rootPageBuf, err := bp.FetchPage(rootPageId)
	if err != nil {
		return err
	}

	overflowKey, overflowChildPageId, leafSplit, err := bt.insertRecursively(bp, rootPageBuf, record)
	if err != nil {
		return err
	}

	rootSplit := !overflowChildPageId.IsInvalid()

	// リーフノードの分割もルートノードの分割も発生しなかった場合、終了
	if !leafSplit && !rootSplit {
		return nil
	}

	// リーフノード・ルートノードの分割が発生した場合 (この二つは同時に発生する可能性もある)
	metaWriteData, err := bp.GetWritePageData(bt.MetaPageId)
	if err != nil {
		return err
	}
	meta = newMetaPage(page.NewPage(metaWriteData))

	// リーフ分割が発生した場合、メタページのリーフページ数を更新
	if leafSplit {
		meta.setLeafPageCount(meta.leafPageCount() + 1)
	}

	// ルートノードの分割が発生した場合、新しいルートノードを作成してメタページを更新
	if rootSplit {
		newRootPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return err
		}
		err = bp.AddPage(newRootPageId)
		if err != nil {
			return err
		}
		newRootData, err := bp.GetWritePageData(newRootPageId)
		if err != nil {
			return err
		}
		newRootBranch := node.NewBranch(page.NewPage(newRootData).Body)
		err = newRootBranch.Initialize(overflowKey, overflowChildPageId, rootPageId)
		if err != nil {
			return err
		}

		meta.setRootPageId(newRootPageId)
		meta.setHeight(meta.height() + 1)
	}

	return nil
}

// insertRecursively は再帰的にノードを辿ってレコードを挿入する
//
// 戻り値: (オーバーフローキー, 新しいページ ID, リーフ分割が発生したか, エラー)
//
// ノード分割が発生しなかった場合、新しいページ ID には InvalidPageId が返る。
// 分割が発生した場合、オーバーフローキーは親ノードに伝播させる境界キーになり、新しいページ ID は分割で作られたノードのページ ID になる。
//
// ※分割挿入発生時にできる新しいノードは、分割元の前に位置するノードとして作られる
func (bt *BTree) insertRecursively(
	bp *buffer.BufferPool,
	nodeBuffer *buffer.BufferPage,
	record node.Record,
) ([]byte, page.PageId, bool, error) {
	nodeData, err := bp.GetReadPageData(nodeBuffer.PageId)
	if err != nil {
		return nil, page.InvalidPageId, false, err
	}
	nodeType := node.GetNodeType(page.NewPage(nodeData).Body)

	switch {
	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		nodeWriteData, err := bp.GetWritePageData(nodeBuffer.PageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		leaf := node.NewLeaf(page.NewPage(nodeWriteData).Body)
		slotNum, found := leaf.SearchSlotNum(record.KeyBytes())
		if found {
			return nil, page.InvalidPageId, false, ErrDuplicateKey
		}

		// リーフノードに挿入できた場合、終了
		if leaf.Insert(slotNum, record) {
			return nil, page.InvalidPageId, false, nil
		}

		// リーフノードが満杯の場合、分割する
		prevLeafPageId := leaf.PrevPageId()

		// 前のリーフノードが存在する場合、そのページを取得
		if prevLeafPageId != nil {
			defer bp.UnRefPage(*prevLeafPageId)
		}

		// 新しいリーフノードを作成
		newLeafPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		err = bp.AddPage(newLeafPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		defer bp.UnRefPage(newLeafPageId)

		// 前のリーフノードが存在する場合、そのリーフノードに格納されている nextPageId を、新しいリーフノードのページID に更新する
		if prevLeafPageId != nil {
			prevLeafData, err := bp.GetWritePageData(*prevLeafPageId)
			if err != nil {
				return nil, page.InvalidPageId, false, err
			}
			prevLeaf := node.NewLeaf(page.NewPage(prevLeafData).Body)
			prevLeaf.SetNextPageId(&newLeafPageId)
		}

		// 新しいリーフノードに分割挿入する
		newLeafData, err := bp.GetWritePageData(newLeafPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		newLeaf := node.NewLeaf(page.NewPage(newLeafData).Body)
		_, err = leaf.SplitInsert(newLeaf, record)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		newLeaf.SetNextPageId(&nodeBuffer.PageId)
		newLeaf.SetPrevPageId(prevLeafPageId)
		leaf.SetPrevPageId(&newLeafPageId)

		// overflowKey は古いリーフノードの先頭のキー (親ノードの境界キーになる)
		overflowKey := leaf.RecordAt(0).KeyBytes()
		return overflowKey, newLeafPageId, true, nil

	case bytes.Equal(nodeType, node.NodeTypeBranch):
		// 挿入先の子ノードを取得
		nodeWriteData, err := bp.GetWritePageData(nodeBuffer.PageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		branch := node.NewBranch(page.NewPage(nodeWriteData).Body)
		childIndex := branch.SearchChildSlotNum(record.KeyBytes())
		childPageId := branch.ChildPageIdAt(childIndex)
		childNodeBuffer, err := bp.FetchPage(childPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		defer bp.UnRefPage(childPageId)

		// 子ノードに対して挿入処理を再帰的に実行
		overflowKeyFromChild, overflowChildPageId, leafSplit, err := bt.insertRecursively(bp, childNodeBuffer, record)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}

		// 子ノードが分割されなかった場合、終了
		childPageSplit := !overflowChildPageId.IsInvalid()
		if !childPageSplit {
			return nil, page.InvalidPageId, leafSplit, nil
		}

		// 子ノードが分割された場合、子ノードから返されたキーとページID をレコードとして、ブランチノードに挿入
		overFlowRecord := node.NewRecord(nil, overflowKeyFromChild, overflowChildPageId.ToBytes())
		if branch.Insert(childIndex, overFlowRecord) {
			return nil, page.InvalidPageId, leafSplit, nil
		}

		// ブランチノードが満杯で挿入に失敗した場合、分割する
		newBranchPageId, err := bp.AllocatePageId(bt.MetaPageId.FileId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		err = bp.AddPage(newBranchPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		defer bp.UnRefPage(newBranchPageId)
		newBranchData, err := bp.GetWritePageData(newBranchPageId)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}
		newBranch := node.NewBranch(page.NewPage(newBranchData).Body)
		overflowKey, err := branch.SplitInsert(newBranch, overFlowRecord)
		if err != nil {
			return nil, page.InvalidPageId, false, err
		}

		return overflowKey, newBranchPageId, leafSplit, nil

	default:
		panic("unknown node type")
	}
}

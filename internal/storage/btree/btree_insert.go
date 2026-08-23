package btree

import (
	"fmt"

	"github.com/ren-yamanashi/minesql/internal/storage/buffer"
	"github.com/ren-yamanashi/minesql/internal/storage/page"
)

// Insert は B+Tree にレコードを挿入する
func (t *Tree) Insert(mtr *buffer.Mtr, record Record) error {
	needsPessimistic, err := t.insertOptimistic(mtr, record)
	if err != nil {
		return err
	}
	if !needsPessimistic {
		return nil
	}
	return t.insertPessimistic(mtr, record)
}

// insertOptimistic は楽観モードで挿入を試みる
//   - 分割が必要と判明した場合は (true, nil) を返し、呼び出し側に悲観モードへの切り替えを促す
func (t *Tree) insertOptimistic(mtr *buffer.Mtr, record Record) (needsPessimistic bool, err error) {
	mtr.LockShared(t.latch)
	defer mtr.UnlockLatch(t.latch)

	pageMeta, err := mtr.PageForRead(t.MetaPageId())
	if err != nil {
		return false, err
	}
	defer mtr.Unpin(t.MetaPageId())
	metaPage := newMetaPage(pageMeta)

	rootPageId := metaPage.rootPageId()
	height := metaPage.height()
	leafBufPage, err := t.descendToLeafExclusive(mtr, rootPageId, height, record.Key())
	if err != nil {
		return false, err
	}

	leafNode := newLeafNode(leafBufPage)
	if !leafNode.canFit(record) {
		return true, nil
	}

	slotNum, found := leafNode.searchSlotNum(record.Key())
	if found {
		return false, ErrDuplicateKey
	}
	leafNode.insert(slotNum, record)
	return false, nil
}

// insertPessimistic は悲観モードで挿入する
func (t *Tree) insertPessimistic(mtr *buffer.Mtr, record Record) error {
	mtr.LockSharedExclusive(t.latch)
	defer mtr.UnlockLatch(t.latch)

	// メタページを取得
	pageMeta, err := mtr.PageForWrite(t.MetaPageId())
	if err != nil {
		return err
	}
	metaPage := newMetaPage(pageMeta)

	// SMO で必要になる最大枚数を事前確保する (leaf 1 + branch height)
	reserved, err := t.reservePages(mtr, metaPage.height())
	if err != nil {
		return err
	}

	if err := t.insertWithMetaUpdate(mtr, metaPage, record, reserved); err != nil {
		t.releaseUnused(mtr, reserved)
		return err
	}
	t.releaseUnused(mtr, reserved)
	return nil
}

// insertWithMetaUpdate はルートから再帰的に挿入し、分割が発生した場合にメタページを更新する
//   - 呼び出し側で B+Tree レベルの SX とメタページの Exclusive を保持していること
//   - reserved: 事前確保済みのページ集合。分割で必要になったページはここから取り出す
func (t *Tree) insertWithMetaUpdate(mtr *buffer.Mtr, mp *metaPage, record Record, reserved *reservedPages) error {
	// ルートページを取得
	rootPageId := mp.rootPageId()
	rootPageBuf, err := mtr.PageForRead(rootPageId)
	if err != nil {
		return err
	}

	// 再帰的に挿入
	overflowKey, overflowChildPageId, isLeafSplit, err := t.insertRecursively(mtr, rootPageBuf, record, reserved)
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
		mp.setLeafPageCount(mp.leafPageCount() + 1)
	}
	if !isRootSplit {
		return nil
	}

	// ルートノードの分割が発生した場合
	newRootPageId := reserved.takeBranch()
	pageNewRoot, err := mtr.PageForWrite(newRootPageId)
	if err != nil {
		panic(fmt.Sprintf("btree: PageForWrite failed for reserved new root page (pageId=%v): %v", newRootPageId, err))
	}
	newRootBranch := newBranchNode(pageNewRoot)
	newRootBranch.initialize(overflowKey, overflowChildPageId, rootPageId)
	mp.setRootPageId(newRootPageId)
	mp.setHeight(mp.height() + 1)
	return nil
}

// descendToLeafExclusive は指定キーに対応するリーフページを Exclusive ラッチ付きで返す
//   - height: B+Tree の高さ (ルート = リーフ なら 1)。これにより、子がリーフ階層かどうかを子を読む前に判定する
//   - 降下中はブランチノードを Shared で持ち替え、リーフの 1 つ上のブランチを処理する時点で子に Exclusive を取り、その後に親を解放する
//   - 戻り値のバッファページは Exclusive ラッチと Pin を Mtr スコープに保持する。呼び出し側は使用後に Unpin する
func (t *Tree) descendToLeafExclusive(mtr *buffer.Mtr, rootPageId page.Id, height uint64, key []byte) (*buffer.Page, error) {
	// 高さ 1 (ルート = リーフ) の場合はルートに直接 Exclusive を取る
	if height <= 1 {
		return mtr.PageForWrite(rootPageId)
	}

	currentPageId := rootPageId
	currentBufPage, err := mtr.PageForRead(currentPageId)
	if err != nil {
		return nil, err
	}
	// currentLevel はルートを height、リーフを 1 とする残り段数
	currentLevel := height
	for {
		branchNode := newBranchNode(currentBufPage)
		childSlotNum, found := branchNode.searchSlotNum(key)
		if found {
			childSlotNum++
		}
		childPageId, err := branchNode.childPageId(childSlotNum)
		if err != nil {
			mtr.Unpin(currentPageId)
			return nil, err
		}

		// 子がリーフ階層なら Exclusive を取り、親を解放して返す (親 S と子 X が一瞬重なる)
		if currentLevel-1 == 1 {
			leafBufPage, err := mtr.PageForWrite(childPageId)
			if err != nil {
				mtr.Unpin(currentPageId)
				return nil, err
			}
			mtr.Unpin(currentPageId)
			return leafBufPage, nil
		}

		// 子がまだブランチ階層なら Shared を取り、親を解放して降下を続ける
		childBufPage, err := mtr.PageForRead(childPageId)
		if err != nil {
			mtr.Unpin(currentPageId)
			return nil, err
		}
		mtr.Unpin(currentPageId)
		currentPageId = childPageId
		currentBufPage = childBufPage
		currentLevel--
	}
}

// insertRecursively は再帰的にノードを辿ってレコードを挿入する
//   - bufPage: 挿入先のノードのバッファページ
//   - record: 挿入するレコード
//   - reserved: 事前確保済みのページ集合。分割で必要になったページはここから取り出す
//   - return:
//   - overflowKey: 分割時の境界キー (分割なしの場合は nil)
//   - newPageId: 分割で作られたノードの PageId (分割なしの場合は InvalidPageId)
//   - isLeafSplit: リーフノードの分割が発生したか
func (t *Tree) insertRecursively(
	mtr *buffer.Mtr,
	bufPage *buffer.Page,
	record Record,
	reserved *reservedPages,
) (overflowKey []byte, newPageId page.Id, isLeafSplit bool, err error) {
	pg, err := mtr.PageForWrite(bufPage.PageId())
	if err != nil {
		return nil, page.InvalidId(), false, err
	}
	nt := nodeType(pg.Data())

	switch nt {
	// ブランチノードの場合: 子ノードに対して再帰実行する
	case nodeTypeBranch:
		// 挿入先の子ノードを取得
		branchNode := newBranchNode(pg)
		childSlotNum, found := branchNode.searchSlotNum(record.Key())
		if found {
			childSlotNum++ // 境界キーと一致する場合、右の子に属する
		}
		childPageId, err := branchNode.childPageId(childSlotNum)
		if err != nil {
			return nil, page.InvalidId(), false, err
		}
		childBufPage, err := mtr.PageForRead(childPageId)
		if err != nil {
			return nil, page.InvalidId(), false, err
		}
		defer mtr.Unpin(childPageId)
		// 子ノードに対して挿入処理を再帰的に実行
		overflowKeyFromChild, overflowChildPageId, isLeafSplit, err := t.insertRecursively(mtr, childBufPage, record, reserved)
		if err != nil {
			return nil, page.InvalidId(), false, err
		}
		// 子ノードが分割されなかった場合、終了
		if overflowChildPageId.IsInvalid() {
			return nil, page.InvalidId(), isLeafSplit, nil
		}
		// 子ノードが分割された場合、ブランチノードにオーバーフローレコードを挿入
		overflowKey, newPageId := t.insertBranchOverflow(
			mtr,
			branchNode,
			childSlotNum,
			overflowKeyFromChild,
			overflowChildPageId,
			reserved,
		)
		return overflowKey, newPageId, isLeafSplit, nil

	// リーフノードの場合: そのまま挿入する
	case nodeTypeLeaf:
		overflowKey, newPageId, err := t.insertLeaf(mtr, bufPage.PageId(), pg, record, reserved)
		if err != nil {
			return nil, page.InvalidId(), false, err
		}
		isSplit := !newPageId.IsInvalid()
		return overflowKey, newPageId, isSplit, nil

	default:
		panic(fmt.Sprintf("btree: unknown node type %q at pageId=%v", nt, bufPage.PageId()))
	}
}

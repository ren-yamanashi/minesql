package btree

import (
	"bytes"
	"minesql/internal/storage/btree/node"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/page"
)

// leafPosition はリーフページ内の位置情報を保持する
type leafPosition struct {
	leafPageId    page.PageId // リーフページの ID
	slotNum       int         // リーフ内のスロット番号 (0-based, SearchSlotNum の結果)
	found         bool        // SearchSlotNum でキーが完全一致したか
	numRecords    int         // リーフ内のレコード数
	parentSlotNum int         // リーフ直上のブランチ内でのスロット番号 (サンプリング推定用)
}

// samplingMaxPages はレンジ分析でリーフをリンクリスト経由で辿る最大ページ数
const samplingMaxPages = 10

// RecordsInRange は lowerKey から upperKey の範囲に含まれるレコード数を推定する
//
// lowerKey/upperKey が nil の場合、先頭/末尾までの全範囲を意味する
//
// leftIncl, rightIncl はそれぞれ境界を含むかどうかを指定する (>= なら true, > なら false)
func (bt *BTree) RecordsInRange(
	bp *buffer.BufferPool,
	lowerKey []byte,
	upperKey []byte,
	leftIncl bool,
	rightIncl bool,
) (int64, error) {
	// 下限位置の特定
	var lowerPos leafPosition
	var err error
	if lowerKey == nil {
		lowerPos, err = bt.findEdgeLeafPosition(bp, false)
	} else {
		lowerPos, err = bt.findLeafPosition(bp, lowerKey)
	}
	if err != nil {
		return 0, err
	}

	// 上限位置の特定
	var upperPos leafPosition
	if upperKey == nil {
		upperPos, err = bt.findEdgeLeafPosition(bp, true)
	} else {
		upperPos, err = bt.findLeafPosition(bp, upperKey)
		// SearchSlotNum は GE (>=) の位置を返す。上限は LE (<=) が必要なので、
		// キーが完全一致しなかった場合は 1 つ前の位置に戻す
		if !upperPos.found {
			upperPos.slotNum--
		}
	}
	if err != nil {
		return 0, err
	}

	// 範囲が空のケース
	if lowerPos.slotNum >= lowerPos.numRecords || upperPos.slotNum < 0 {
		return 0, nil
	}

	// 同一リーフページの場合
	if lowerPos.leafPageId == upperPos.leafPageId {
		return bt.estimateSamePage(lowerPos, upperPos, leftIncl, rightIncl), nil
	}

	// 異なるリーフページの場合: リンクリストを辿って距離を確認
	return bt.estimateDifferentPages(bp, lowerPos, upperPos, leftIncl, rightIncl)
}

// estimateSamePage は同一リーフページ内でのレコード数を算出する
//
// cost.md の式: nth_rec_2 - nth_rec_1 - 1 + left_incl + right_incl
//
// 実装では等価な式 (upper.slotNum - lower.slotNum + 1 - !leftIncl - !rightIncl) を使用
//
// 境界除外 (!leftIncl / !rightIncl) はキーが完全一致した場合のみ適用する。
//
// キーが存在しない場合、GE 位置は既に「最初の record > key」を指すため、
// 追加で除外すると 1 つ余分にスキップしてしまう (上限側の LE 調整後も同様)
func (bt *BTree) estimateSamePage(lower, upper leafPosition, leftIncl, rightIncl bool) int64 {
	count := int64(upper.slotNum - lower.slotNum + 1)
	if !leftIncl && lower.found {
		count--
	}
	if !rightIncl && upper.found {
		count--
	}
	if count < 0 {
		return 0
	}
	return count
}

// estimateDifferentPages は異なるリーフページにまたがる場合のレコード数を推定する
//
// 下限ページから NextPageId を辿り、上限ページとの距離に応じて分岐する
//   - 隣接ページ: 正確なカウント
//   - 10 ページ以内: 中間ページのレコード数を合算
//   - 10 ページ超: サンプリングベース推定
func (bt *BTree) estimateDifferentPages(
	bp *buffer.BufferPool,
	lower, upper leafPosition,
	leftIncl, rightIncl bool,
) (int64, error) {
	// 下限ページの末尾までのレコード数
	countFromLower := int64(lower.numRecords - lower.slotNum)
	if !leftIncl && lower.found {
		countFromLower--
	}

	// 上限ページの先頭からのレコード数
	countFromUpper := int64(upper.slotNum + 1)
	if !rightIncl && upper.found {
		countFromUpper--
	}

	// 下限ページから上限ページまでリンクリストを辿る
	var midRecordsTotal int64
	pagesRead := 0
	currentPageId := lower.leafPageId
	foundUpper := false

	for range samplingMaxPages {
		nextPageId, numRecs, err := bt.readNextLeafInfo(bp, currentPageId)
		if err != nil {
			return 0, err
		}
		if nextPageId == nil {
			// リンクリストの末尾に到達
			break
		}
		pagesRead++

		if *nextPageId == upper.leafPageId {
			foundUpper = true
			break
		}

		// 中間ページのレコード数を加算
		midRecordsTotal += int64(numRecs)
		currentPageId = *nextPageId
	}

	if foundUpper {
		// 10 ページ以内に上限ページが見つかった場合: 正確なカウント
		return countFromLower + midRecordsTotal + countFromUpper, nil
	}

	// 10 ページ超: サンプリングベース推定
	return bt.estimateBySampling(bp, lower, upper, midRecordsTotal, pagesRead, leftIncl, rightIncl)
}

// estimateBySampling はサンプリングベースでレコード数を推定する
//
// ブランチノードのスロット位置差分 (対象範囲のリーフページ数) × ページあたり平均レコード数
func (bt *BTree) estimateBySampling(
	bp *buffer.BufferPool,
	lower, upper leafPosition,
	sampleRecords int64,
	samplePages int,
	leftIncl, rightIncl bool,
) (int64, error) {
	// サンプルページに下限ページ自体のレコード数も含める
	totalSampleRecords := sampleRecords + int64(lower.numRecords)
	totalSamplePages := samplePages + 1

	avgRecsPerPage := float64(totalSampleRecords) / float64(totalSamplePages)

	// ブランチノードのスロット位置差分 = 対象範囲のリーフページ数の推定
	nRowsOnPrevLevel := upper.parentSlotNum - lower.parentSlotNum
	nRowsOnPrevLevel = max(nRowsOnPrevLevel, 1)

	estimate := int64(float64(nRowsOnPrevLevel) * avgRecsPerPage)

	// 境界調整 (キーが完全一致した場合のみ。存在しないキーでは GE/LE 位置が
	// 既に正しいレコードを指しているため、追加の調整は不要)
	if leftIncl && lower.found {
		estimate++
	}
	if rightIncl && upper.found {
		estimate++
	}

	// 補正: B+Tree の高さが 1 より大きい場合、推定値を 2 倍にする
	height, err := bt.Height(bp)
	if err != nil {
		return 0, err
	}
	if height > 1 {
		estimate *= 2
	}

	// 上限: テーブル総行数の半分 (LeafPageCount × 平均レコード数で近似)
	leafCount, err := bt.LeafPageCount(bp)
	if err != nil {
		return 0, err
	}
	totalRowsEstimate := int64(float64(leafCount) * avgRecsPerPage)
	if maxEstimate := totalRowsEstimate / 2; estimate > maxEstimate && maxEstimate > 0 {
		estimate = maxEstimate
	}

	if estimate < 0 {
		return 0, nil
	}
	return estimate, nil
}

// readNextLeafInfo は指定リーフの次のリーフページの ID とレコード数を返す
//
// 内部で指定リーフ (NextPageId の取得) と次のリーフ (レコード数の取得) の 2 ページを読む
// 次のリーフが存在しない場合は (nil, 0, nil) を返す
func (bt *BTree) readNextLeafInfo(bp *buffer.BufferPool, leafPageId page.PageId) (*page.PageId, int, error) {
	data, err := bp.GetReadPageData(leafPageId)
	if err != nil {
		return nil, 0, err
	}
	bp.UnRefPage(leafPageId)

	leaf := node.NewLeaf(page.NewPage(data).Body)
	nextPageId := leaf.NextPageId()
	if nextPageId == nil {
		return nil, 0, nil
	}

	// 次のリーフページのレコード数を取得
	nextData, err := bp.GetReadPageData(*nextPageId)
	if err != nil {
		return nil, 0, err
	}
	bp.UnRefPage(*nextPageId)

	nextLeaf := node.NewLeaf(page.NewPage(nextData).Body)
	return nextPageId, nextLeaf.NumRecords(), nil
}

// findLeafPosition はルートからリーフまで探索し、指定キーのリーフ位置を返す
func (bt *BTree) findLeafPosition(bp *buffer.BufferPool, key []byte) (leafPosition, error) {
	defer bp.UnRefPage(bt.MetaPageId)
	metaData, err := bp.GetReadPageData(bt.MetaPageId)
	if err != nil {
		return leafPosition{}, err
	}
	meta := newMetaPage(page.NewPage(metaData))
	rootPageId := meta.rootPageId()

	return bt.findLeafPositionRecursively(bp, rootPageId, key, 0)
}

func (bt *BTree) findLeafPositionRecursively(bp *buffer.BufferPool, nodePageId page.PageId, key []byte, parentSlotNum int) (leafPosition, error) {
	nodeData, err := bp.GetReadPageData(nodePageId)
	if err != nil {
		return leafPosition{}, err
	}
	nodeType := node.GetNodeType(page.NewPage(nodeData).Body)

	switch {
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		defer bp.UnRefPage(nodePageId)
		branch := node.NewBranch(page.NewPage(nodeData).Body)
		childSlot := branch.SearchChildSlotNum(key)
		childPageId := branch.ChildPageIdAt(childSlot)
		return bt.findLeafPositionRecursively(bp, childPageId, key, childSlot)

	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		bp.UnRefPage(nodePageId)
		leaf := node.NewLeaf(page.NewPage(nodeData).Body)
		slotNum, found := leaf.SearchSlotNum(key)

		// キーがリーフの全レコードより大きい場合、次のリーフの先頭に進む
		// (ブランチの区切りキーとの間に落ちるキーで発生しうる)
		if slotNum >= leaf.NumRecords() && leaf.NextPageId() != nil {
			nextPageId := *leaf.NextPageId()
			nextData, err := bp.GetReadPageData(nextPageId)
			if err != nil {
				return leafPosition{}, err
			}
			bp.UnRefPage(nextPageId)
			nextLeaf := node.NewLeaf(page.NewPage(nextData).Body)
			return leafPosition{
				leafPageId:    nextPageId,
				slotNum:       0,
				found:         false,
				numRecords:    nextLeaf.NumRecords(),
				parentSlotNum: parentSlotNum + 1,
			}, nil
		}

		return leafPosition{
			leafPageId:    nodePageId,
			slotNum:       slotNum,
			found:         found,
			numRecords:    leaf.NumRecords(),
			parentSlotNum: parentSlotNum,
		}, nil

	default:
		panic("unknown node type")
	}
}

// findEdgeLeafPosition は最左 (findLast=false) または最右 (findLast=true) のリーフ位置を返す
func (bt *BTree) findEdgeLeafPosition(bp *buffer.BufferPool, findLast bool) (leafPosition, error) {
	defer bp.UnRefPage(bt.MetaPageId)
	metaData, err := bp.GetReadPageData(bt.MetaPageId)
	if err != nil {
		return leafPosition{}, err
	}
	meta := newMetaPage(page.NewPage(metaData))
	rootPageId := meta.rootPageId()

	return bt.findEdgeLeafPositionRecursively(bp, rootPageId, findLast, 0)
}

func (bt *BTree) findEdgeLeafPositionRecursively(bp *buffer.BufferPool, nodePageId page.PageId, findLast bool, parentSlotNum int) (leafPosition, error) {
	nodeData, err := bp.GetReadPageData(nodePageId)
	if err != nil {
		return leafPosition{}, err
	}
	nodeType := node.GetNodeType(page.NewPage(nodeData).Body)

	switch {
	case bytes.Equal(nodeType, node.NodeTypeBranch):
		defer bp.UnRefPage(nodePageId)
		branch := node.NewBranch(page.NewPage(nodeData).Body)
		var childSlot int
		if findLast {
			childSlot = branch.NumRecords() // 右端の子
		} else {
			childSlot = 0 // 左端の子
		}
		childPageId := branch.ChildPageIdAt(childSlot)
		return bt.findEdgeLeafPositionRecursively(bp, childPageId, findLast, childSlot)

	case bytes.Equal(nodeType, node.NodeTypeLeaf):
		bp.UnRefPage(nodePageId)
		leaf := node.NewLeaf(page.NewPage(nodeData).Body)
		slotNum := 0
		if findLast {
			slotNum = max(leaf.NumRecords()-1, 0)
		}
		return leafPosition{
			leafPageId:    nodePageId,
			slotNum:       slotNum,
			found:         true,
			numRecords:    leaf.NumRecords(),
			parentSlotNum: parentSlotNum,
		}, nil

	default:
		panic("unknown node type")
	}
}

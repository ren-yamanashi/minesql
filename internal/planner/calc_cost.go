package planner

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/handler"
)

// RowEvaluateCost は 1 レコードを評価するコスト (MySQL の row_evaluate_cost に対応)
const RowEvaluateCost = 0.1

// -----------------------------------------------
// コスト算出
// -----------------------------------------------

// calcFullScanReadCost はフルテーブルスキャンの I/O コスト (readCost) のみを返す
//
// readCost = scanTime × pageReadCost
//
// RowEvaluateCost を含まない。JOIN の prefix_cost 累積で二重加算を防ぐため分離している
func calcFullScanReadCost(stats *handler.TableStatistics, pageReadCost float64) float64 {
	scanTime := float64(stats.LeafPageCount)
	return scanTime * pageReadCost
}

// calcFullScanCost はフルテーブルスキャンの最終コストを算出する
//
// cost = readCost + foundRecords × RowEvaluateCost
//
// = scanTime × pageReadCost + foundRecords × RowEvaluateCost
func calcFullScanCost(stats *handler.TableStatistics, pageReadCost float64) float64 {
	readCost := calcFullScanReadCost(stats, pageReadCost)
	foundRecords := float64(stats.RecordCount)
	return readCost + foundRecords*RowEvaluateCost
}

// calcUniqueScanCost はユニークスキャン (UNIQUE KEY or PK で = 検索) のコストを返す
//
// MySQL と同様にコスト 1.0 固定
func calcUniqueScanCost() float64 {
	return 1.0
}

// calcRangeScanCost はレンジスキャンの最終コストを算出する
//
// rangeCost = readTime + foundRecords × RowEvaluateCost + 0.01
// 最終コスト = rangeCost + foundRecords × RowEvaluateCost
//
//	= readTime + 2 × foundRecords × RowEvaluateCost + 0.01
func calcRangeScanCost(readTime float64, foundRecords float64) float64 {
	return readTime + 2*foundRecords*RowEvaluateCost + 0.01
}

// -----------------------------------------------
// readTime 算出
// -----------------------------------------------

// calcReadTimeForSecondaryIndex はセカンダリインデックス (非 index-only) の readTime を算出する
//
// readTime = (nRanges + foundRecords) × pageReadCost
func calcReadTimeForSecondaryIndex(nRanges int, foundRecords float64, pageReadCost float64) float64 {
	return (float64(nRanges) + foundRecords) * pageReadCost
}

// calcReadTimeForClusteredIndex はクラスタ化インデックスの readTime を算出する
//
// foundRecords <= 2 の場合: readTime = foundRecords × pageReadCost
// それ以外: readTime = (nRanges + (foundRecords / totalRows) × scanTime) × pageReadCost
func calcReadTimeForClusteredIndex(nRanges int, foundRecords float64, totalRows float64, scanTime float64, pageReadCost float64) float64 {
	if foundRecords <= 2 {
		return foundRecords * pageReadCost
	}
	ratio := foundRecords / totalRows
	return (float64(nRanges) + ratio*scanTime) * pageReadCost
}

// -----------------------------------------------
// page_read_cost
// -----------------------------------------------

// calcPageReadCost はバッファプールのキャッシュ率から page_read_cost を算出する
//
// page_read_cost = in_mem × 0.25 + (1 - in_mem) × 1.0
// in_mem = バッファプール内のリーフページ数 / 総リーフページ数
//
// リーフページ自体は読まず、ブランチから PageId を列挙して IsPageCached で判定する
func calcPageReadCost(bp *buffer.BufferPool, bt *btree.BTree) (float64, error) {
	leafPageIds, err := bt.LeafPageIds(bp)
	if err != nil {
		return 0, err
	}

	nLeaf := len(leafPageIds)
	if nLeaf == 0 {
		return 1.0, nil
	}

	nInMem := 0
	for _, pageId := range leafPageIds {
		if bp.IsPageCached(pageId) {
			nInMem++
		}
	}

	inMem := float64(nInMem) / float64(nLeaf)
	return inMem*0.25 + (1-inMem)*1.0, nil
}

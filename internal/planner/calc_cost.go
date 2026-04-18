package planner

import (
	"minesql/internal/storage/btree"
	"minesql/internal/storage/buffer"
	"minesql/internal/storage/handler"
)

// defaultRangeSelectivity は min/max が不明な場合の範囲比較の推定選択率
const defaultRangeSelectivity = 1.0 / 3.0

// コストモデルの重み定数
//
// ref: https://dev.mysql.com/doc/refman/8.0/en/cost-model.html
const (
	weightIOBlockRead = 1.0 // ディスクからのブロック読み取り 1 回あたりのコスト
	weightRowEvaluate = 0.1 // レコード 1 件の条件評価 (CPU) あたりのコスト
)

// ScanCost はスキャンのコスト見積もり
type ScanCost struct {
	DiskAccesses float64            // B(s): ディスクアクセス数
	RecordCount  float64            // R(s): 結果レコード数
	UniqueValues map[string]float64 // V(s, F): 各カラムの異なる値の数
}

// TotalCost は I/O コストと CPU コストを重み付けした総合コストを返す
func (c ScanCost) TotalCost() float64 {
	return c.DiskAccesses*weightIOBlockRead + c.RecordCount*weightRowEvaluate
}

// -----------------------------------------------
// テーブルスキャン
// -----------------------------------------------

// calcTableScanCost はテーブルスキャンのコストを算出する
//
// B(s) = B(T), R(s) = R(T), V(s,F) = V(T,F)
func calcTableScanCost(stats *handler.TableStatistics) ScanCost {
	uv := make(map[string]float64, len(stats.ColStats))
	for col, cs := range stats.ColStats {
		uv[col] = float64(cs.UniqueValues)
	}
	return ScanCost{
		DiskAccesses: float64(stats.LeafPageCount),
		RecordCount:  float64(stats.RecordCount),
		UniqueValues: uv,
	}
}

// -----------------------------------------------
// 選択スキャン (PK・インデックスなし)
// -----------------------------------------------

// calcSelectEqualCost は等価比較 (A = c) の選択スキャンのコストを算出する
//
// colName: 条件カラム A
//
// B(s) = B(s1)
//
// R(s) = R(s1) / V(s1,A)
//
// V(s,F) = if F=A then 1 else V(s1,F)
func calcSelectEqualCost(inner ScanCost, colName string) ScanCost {
	uv := copyUniqueValues(inner.UniqueValues)
	uv[colName] = 1

	vA := inner.UniqueValues[colName]
	recordCount := inner.RecordCount
	if vA > 0 {
		recordCount /= vA
	}

	return ScanCost{
		DiskAccesses: inner.DiskAccesses,
		RecordCount:  recordCount,
		UniqueValues: uv,
	}
}

// calcSelectNotEqualCost は非等価比較 (A != c) の選択スキャンのコストを算出する
//
// colName: 条件カラム A
//
// B(s) = B(s1)
//
// R(s) = R(s1) * (V(s1,A)-1) / V(s1,A)
//
// V(s,F) = if F=A then V(s1,A)-1 else V(s1,F)
func calcSelectNotEqualCost(inner ScanCost, colName string) ScanCost {
	uv := copyUniqueValues(inner.UniqueValues)
	vA := inner.UniqueValues[colName]

	// F = A の場合
	if vA > 1 {
		uv[colName] = vA - 1
	}

	recordCount := inner.RecordCount
	if vA > 0 {
		recordCount = inner.RecordCount * (vA - 1) / vA
	}

	return ScanCost{
		DiskAccesses: inner.DiskAccesses,
		RecordCount:  recordCount,
		UniqueValues: uv,
	}
}

// calcSelectRangeCost は範囲比較 (A > c, A < c 等) の選択スキャンのコストを算出する
//
// colName: 条件カラム A
//
// selectivity: 条件に該当する値域の割合 (0.0〜1.0)。calcRangeSelectivity で算出できる
//
// B(s) = B(s1)
//
// R(s) = R(s1) * selectivity
//
// V(s,F) = if F=A then V(s1,A) * selectivity else V(s1,F)
func calcSelectRangeCost(inner ScanCost, colName string, selectivity float64) ScanCost {
	uv := copyUniqueValues(inner.UniqueValues)
	uv[colName] = inner.UniqueValues[colName] * selectivity

	return ScanCost{
		DiskAccesses: inner.DiskAccesses,
		RecordCount:  inner.RecordCount * selectivity,
		UniqueValues: uv,
	}
}

// -----------------------------------------------
// 選択スキャン (PK)
// -----------------------------------------------

// calcPKSelectEqualCost は PK の等価比較 (pk = c) の選択スキャンのコストを算出する
//
// B+Tree の二分探索で直接到達できるため B(s) = H(T)
//
// colName: 条件カラム A (PK の先頭カラム)
//
// primaryHeight: プライマリ B+Tree の高さ = H(T)
//
// B(s) = H(T)
//
// R(s) = R(s1) / V(s1,A)
//
// V(s,F) = if F=A then 1 else V(s1,F)
func calcPKSelectEqualCost(inner ScanCost, colName string, primaryHeight uint64) ScanCost {
	cost := calcSelectEqualCost(inner, colName)
	cost.DiskAccesses = float64(primaryHeight)
	return cost
}

// calcPKSelectRangeGTCost は PK の範囲比較 (>, >=) の選択スキャンのコストを算出する
//
// B+Tree の二分探索で開始位置にシークし、そこから末尾方向へリーフを走査する
//
// colName: 条件カラム A (PK の先頭カラム)
//
// selectivity: 条件に該当する値域の割合 (0.0〜1.0)
//
// primaryHeight: プライマリ B+Tree の高さ = H(T)
//
// B(s) = H(T) + sel * B(T)
//
// R(s) = R(s1) * sel
//
// V(s,F) = if F=A then V(s1,A) * sel else V(s1,F)
func calcPKSelectRangeGTCost(inner ScanCost, colName string, selectivity float64, primaryHeight uint64) ScanCost {
	cost := calcSelectRangeCost(inner, colName, selectivity)
	cost.DiskAccesses = float64(primaryHeight) + selectivity*inner.DiskAccesses
	return cost
}

// calcPKSelectRangeLTCost は PK の範囲比較 (<, <=) の選択スキャンのコストを算出する
//
// 先頭から走査して条件を満たさなくなった時点で終了する
//
// colName: 条件カラム A (PK の先頭カラム)
//
// selectivity: 条件に該当する値域の割合 (0.0〜1.0)
//
// B(s) = sel * B(T)
//
// R(s) = R(s1) * sel
//
// V(s,F) = if F=A then V(s1,A) * sel else V(s1,F)
func calcPKSelectRangeLTCost(inner ScanCost, colName string, selectivity float64) ScanCost {
	cost := calcSelectRangeCost(inner, colName, selectivity)
	cost.DiskAccesses = selectivity * inner.DiskAccesses
	return cost
}

// -----------------------------------------------
// 射影スキャン
// -----------------------------------------------

// calcProjectCost は射影スキャン (SELECT 句でのカラム指定) のコストを算出する
//
// B(s) = B(s1)
//
// R(s) = R(s1)
//
// V(s,F) = V(s1,F)
func calcProjectCost(inner ScanCost) ScanCost {
	return ScanCost{
		DiskAccesses: inner.DiskAccesses,
		RecordCount:  inner.RecordCount,
		UniqueValues: copyUniqueValues(inner.UniqueValues),
	}
}

// -----------------------------------------------
// インデックススキャン
// -----------------------------------------------

// calcIndexScanCost はインデックススキャンのコストを算出する
//
// indexHeight: インデックス I の B+Tree の高さ = H(I)
//
// B(s) = H(I)
//
// R(s) = R(T)
//
// V(s,F) = V(T,F)
func calcIndexScanCost(stats *handler.TableStatistics, indexHeight uint64) ScanCost {
	uv := make(map[string]float64, len(stats.ColStats))
	for col, cs := range stats.ColStats {
		uv[col] = float64(cs.UniqueValues)
	}
	return ScanCost{
		DiskAccesses: float64(indexHeight),
		RecordCount:  float64(stats.RecordCount),
		UniqueValues: uv,
	}
}

// -----------------------------------------------
// 選択スキャン (インデックス)
// -----------------------------------------------

// calcIndexSelectEqualCost はインデックスの等価比較 (A = c) のコストを算出する
//
// colName: 条件カラム A
//
// indexHeight: インデックス I の B+Tree の高さ = H(I)
//
// B(s) = H(I)
//
// R(s) = R(T) / V(T,A)
//
// V(s,F) = if F=A then 1 else V(T,F)
func calcIndexSelectEqualCost(stats *handler.TableStatistics, colName string, indexHeight uint64) ScanCost {
	uv := make(map[string]float64, len(stats.ColStats))
	for col, cs := range stats.ColStats {
		if col == colName {
			uv[col] = 1
		} else {
			uv[col] = float64(cs.UniqueValues)
		}
	}

	vA := float64(stats.ColStats[colName].UniqueValues)
	recordCount := float64(stats.RecordCount)
	if vA > 0 {
		recordCount /= vA
	}

	return ScanCost{
		DiskAccesses: float64(indexHeight),
		RecordCount:  recordCount,
		UniqueValues: uv,
	}
}

// calcIndexSelectNotEqualCost はインデックスの非等価比較 (A != c) のコストを算出する
//
// colName: 条件カラム A
//
// indexHeight: インデックス I の B+Tree の高さ = H(I)
//
// indexLeafPages: インデックス I のリーフページ数 = Bl(I)
//
// B(s) = H(I) + Bl(I)
//
// R(s) = R(T) * (V(T,A)-1) / V(T,A)
//
// V(s,F) = if F=A then V(T,A)-1 else V(T,F)
func calcIndexSelectNotEqualCost(stats *handler.TableStatistics, colName string, indexHeight uint64, indexLeafPages uint64) ScanCost {
	uv := make(map[string]float64, len(stats.ColStats))
	for col, cs := range stats.ColStats {
		if col == colName {
			v := float64(cs.UniqueValues)
			if v > 1 {
				uv[col] = v - 1
			} else {
				uv[col] = v
			}
		} else {
			uv[col] = float64(cs.UniqueValues)
		}
	}

	vA := float64(stats.ColStats[colName].UniqueValues)
	recordCount := float64(stats.RecordCount)
	if vA > 0 {
		recordCount = float64(stats.RecordCount) * (vA - 1) / vA
	}

	return ScanCost{
		DiskAccesses: float64(indexHeight) + float64(indexLeafPages),
		RecordCount:  recordCount,
		UniqueValues: uv,
	}
}

// calcIndexSelectRangeCost はインデックスの範囲比較 (A > c 等) のコストを算出する
//
// colName: 条件カラム A
//
// indexHeight: インデックス I の B+Tree の高さ = H(I)
//
// indexLeafPages: インデックス I のリーフページ数 = Bl(I)
//
// selectivity: 条件に該当する値域の割合 (0.0〜1.0)
//
// B(s) = H(I) + Bl(I) * selectivity
//
// R(s) = R(T) * selectivity
//
// V(s,F) = if F=A then V(T,A) * selectivity else V(T,F)
func calcIndexSelectRangeCost(stats *handler.TableStatistics, colName string, indexHeight uint64, indexLeafPages uint64, selectivity float64) ScanCost {
	uv := make(map[string]float64, len(stats.ColStats))
	for col, cs := range stats.ColStats {
		if col == colName {
			uv[col] = float64(cs.UniqueValues) * selectivity
		} else {
			uv[col] = float64(cs.UniqueValues)
		}
	}

	return ScanCost{
		DiskAccesses: float64(indexHeight) + float64(indexLeafPages)*selectivity,
		RecordCount:  float64(stats.RecordCount) * selectivity,
		UniqueValues: uv,
	}
}

// -----------------------------------------------
// インデックス + テーブル (Primary Lookup)
// -----------------------------------------------

// calcIndexTableEqualCost はインデックス + テーブルの等価比較 (A = c) のコストを算出する
//
// colName: 条件カラム A
//
// indexHeight: インデックス I の B+Tree の高さ = H(I)
//
// primaryHeight: テーブル T のプライマリキー B+Tree の高さ = H(T)
//
// B(s) = H(I) + R(s) * H(T)
//
// R(s) = R(T) / V(T,A)
//
// V(s,F) = if F=A then 1 else V(T,F)
func calcIndexTableEqualCost(stats *handler.TableStatistics, colName string, indexHeight uint64, primaryHeight uint64) ScanCost {
	uv := make(map[string]float64, len(stats.ColStats))
	for col, cs := range stats.ColStats {
		if col == colName {
			uv[col] = 1
		} else {
			uv[col] = float64(cs.UniqueValues)
		}
	}

	vA := float64(stats.ColStats[colName].UniqueValues)
	recordCount := float64(stats.RecordCount)
	if vA > 0 {
		recordCount /= vA
	}

	return ScanCost{
		DiskAccesses: float64(indexHeight) + recordCount*float64(primaryHeight),
		RecordCount:  recordCount,
		UniqueValues: uv,
	}
}

// calcIndexTableNotEqualCost はインデックス + テーブルの非等価比較 (A != c) のコストを算出する
//
// colName: 条件カラム A
//
// indexHeight: インデックス I の B+Tree の高さ = H(I)
//
// indexLeafPages: インデックス I のリーフページ数 = Bl(I)
//
// primaryHeight: テーブル T のプライマリキー B+Tree の高さ = H(T)
//
// B(s) = H(I) + Bl(I) + R(s) * H(T)
//
// R(s) = R(T) * (V(T,A)-1) / V(T,A)
//
// V(s,F) = if F=A then V(T,A)-1 else V(T,F)
func calcIndexTableNotEqualCost(stats *handler.TableStatistics, colName string, indexHeight uint64, indexLeafPages uint64, primaryHeight uint64) ScanCost {
	uv := make(map[string]float64, len(stats.ColStats))
	for col, cs := range stats.ColStats {
		if col == colName {
			v := float64(cs.UniqueValues)
			if v > 1 {
				uv[col] = v - 1
			} else {
				uv[col] = v
			}
		} else {
			uv[col] = float64(cs.UniqueValues)
		}
	}

	vA := float64(stats.ColStats[colName].UniqueValues)
	recordCount := float64(stats.RecordCount)
	if vA > 0 {
		recordCount = float64(stats.RecordCount) * (vA - 1) / vA
	}

	return ScanCost{
		DiskAccesses: float64(indexHeight) + float64(indexLeafPages) + recordCount*float64(primaryHeight),
		RecordCount:  recordCount,
		UniqueValues: uv,
	}
}

// calcIndexTableRangeCost はインデックス + テーブルの範囲比較 (A > c 等) のコストを算出する
//
// colName: 条件カラム A
//
// indexHeight: インデックス I の B+Tree の高さ = H(I)
//
// indexLeafPages: インデックス I のリーフページ数 = Bl(I)
//
// selectivity: 条件に該当する値域の割合 (0.0〜1.0)
//
// primaryHeight: テーブル T のプライマリキー B+Tree の高さ = H(T)
//
// B(s) = H(I) + Bl(I) * selectivity + R(s) * H(T)
//
// R(s) = R(T) * selectivity
//
// V(s,F) = if F=A then V(T,A) * selectivity else V(T,F)
func calcIndexTableRangeCost(stats *handler.TableStatistics, colName string, indexHeight uint64, indexLeafPages uint64, selectivity float64, primaryHeight uint64) ScanCost {
	uv := make(map[string]float64, len(stats.ColStats))
	for col, cs := range stats.ColStats {
		if col == colName {
			uv[col] = float64(cs.UniqueValues) * selectivity
		} else {
			uv[col] = float64(cs.UniqueValues)
		}
	}

	recordCount := float64(stats.RecordCount) * selectivity

	return ScanCost{
		DiskAccesses: float64(indexHeight) + float64(indexLeafPages)*selectivity + recordCount*float64(primaryHeight),
		RecordCount:  recordCount,
		UniqueValues: uv,
	}
}

// -----------------------------------------------
// 新コストモデル (cost2.md 準拠)
// -----------------------------------------------

// RowEvaluateCost は 1 レコードを評価するコスト (MySQL の row_evaluate_cost に対応)
const RowEvaluateCost = 0.1

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
//      = scanTime × pageReadCost + foundRecords × RowEvaluateCost
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
//            = readTime + 2 × foundRecords × RowEvaluateCost + 0.01
func calcRangeScanCost(readTime float64, foundRecords float64) float64 {
	return readTime + 2*foundRecords*RowEvaluateCost + 0.01
}

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

// -----------------------------------------------
// その他
// -----------------------------------------------

// calcRangeSelectivity は範囲比較の選択率を算出する
//
// operator: 比較演算子 (">", ">=", "<", "<=")
//
// c: WHERE 句の比較値
//
// minVal: カラムの最小値
//
// maxVal: カラムの最大値
//
// min/max が等しい場合は defaultRangeSelectivity (1/3) を返す
func calcRangeSelectivity(operator string, c, minVal, maxVal float64) float64 {
	if maxVal == minVal {
		return defaultRangeSelectivity
	}

	var sel float64
	switch operator {
	case ">", ">=":
		sel = (maxVal - c) / (maxVal - minVal)
	case "<", "<=":
		sel = (c - minVal) / (maxVal - minVal)
	default:
		return defaultRangeSelectivity
	}

	// 0.0〜1.0 にクランプ
	if sel < 0 {
		return 0
	}
	if sel > 1 {
		return 1
	}
	return sel
}

func copyUniqueValues(src map[string]float64) map[string]float64 {
	dst := make(map[string]float64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
